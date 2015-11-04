/*******************************************************************************
 * thrill/io/request_queue_impl_qwqr.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2005 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <algorithm>

#include "error_handling.hpp"
#include <thrill/common/logger.hpp>
#include <thrill/io/request_queue_impl_qwqr.hpp>
#include <thrill/io/serving_request.hpp>

#if STXXL_STD_THREADS && STXXL_MSVC >= 1700
 #include <windows.h>
#endif

#ifndef STXXL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
#define STXXL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION 1
#endif

namespace thrill {
namespace io {

struct file_offset_match : public std::binary_function<request_ptr, request_ptr, bool>
{
    bool operator () (
        const request_ptr& a,
        const request_ptr& b) const {
        // matching file and offset are enough to cause problems
        return (a->get_offset() == b->get_offset()) &&
               (a->get_file() == b->get_file());
    }
};

request_queue_impl_qwqr::request_queue_impl_qwqr(int n)
    : m_thread_state(NOT_RUNNING), m_sem(0) {
    common::THRILL_UNUSED(n);
    start_thread(worker, static_cast<void*>(this), m_thread, m_thread_state);
}

void request_queue_impl_qwqr::add_request(request_ptr& req) {
    if (req.empty())
        STXXL_THROW_INVALID_ARGUMENT("Empty request submitted to disk_queue.");
    if (m_thread_state() != RUNNING)
        STXXL_THROW_INVALID_ARGUMENT("Request submitted to not running queue.");
    if (!dynamic_cast<serving_request*>(req.get()))
        LOG1 << "Incompatible request submitted to running queue.";

    if (req.get()->get_type() == request::READ)
    {
#if STXXL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
        {
            std::unique_lock<std::mutex> Lock(m_write_mutex);
            if (std::find_if(m_write_queue.begin(), m_write_queue.end(),
                             bind2nd(file_offset_match(), req))
                != m_write_queue.end())
            {
                LOG1 << "READ request submitted for a BID with a pending WRITE request";
            }
        }
#endif
        std::unique_lock<std::mutex> Lock(m_read_mutex);
        m_read_queue.push_back(req);
    }
    else
    {
#if STXXL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
        {
            std::unique_lock<std::mutex> Lock(m_read_mutex);
            if (std::find_if(m_read_queue.begin(), m_read_queue.end(),
                             bind2nd(file_offset_match(), req))
                != m_read_queue.end())
            {
                LOG1 << "WRITE request submitted for a BID with a pending READ request";
            }
        }
#endif
        std::unique_lock<std::mutex> Lock(m_write_mutex);
        m_write_queue.push_back(req);
    }

    m_sem++;
}

bool request_queue_impl_qwqr::cancel_request(request_ptr& req) {
    if (req.empty())
        STXXL_THROW_INVALID_ARGUMENT("Empty request canceled disk_queue.");
    if (m_thread_state() != RUNNING)
        STXXL_THROW_INVALID_ARGUMENT("Request canceled to not running queue.");
    if (!dynamic_cast<serving_request*>(req.get()))
        LOG1 << "Incompatible request submitted to running queue.";

    bool was_still_in_queue = false;
    if (req.get()->get_type() == request::READ)
    {
        std::unique_lock<std::mutex> Lock(m_read_mutex);
        queue_type::iterator pos
            = std::find(m_read_queue.begin(), m_read_queue.end(), req);
        if (pos != m_read_queue.end())
        {
            m_read_queue.erase(pos);
            was_still_in_queue = true;
            m_sem--;
        }
    }
    else
    {
        std::unique_lock<std::mutex> Lock(m_write_mutex);
        queue_type::iterator pos
            = std::find(m_write_queue.begin(), m_write_queue.end(), req);
        if (pos != m_write_queue.end())
        {
            m_write_queue.erase(pos);
            was_still_in_queue = true;
            m_sem--;
        }
    }

    return was_still_in_queue;
}

request_queue_impl_qwqr::~request_queue_impl_qwqr() {
    stop_thread(m_thread, m_thread_state, m_sem);
}

void* request_queue_impl_qwqr::worker(void* arg) {
    self* pthis = static_cast<self*>(arg);

    bool write_phase = true;
    for ( ; ; )
    {
        pthis->m_sem--;

        if (write_phase)
        {
            std::unique_lock<std::mutex> WriteLock(pthis->m_write_mutex);
            if (!pthis->m_write_queue.empty())
            {
                request_ptr req = pthis->m_write_queue.front();
                pthis->m_write_queue.pop_front();

                WriteLock.unlock();

                // assert(req->get_reference_count()) > 1);
                dynamic_cast<serving_request*>(req.get())->serve();
            }
            else
            {
                WriteLock.unlock();

                pthis->m_sem++;

                if (pthis->m_priority_op == WRITE)
                    write_phase = false;
            }

            if (pthis->m_priority_op == NONE || pthis->m_priority_op == READ)
                write_phase = false;
        }
        else
        {
            std::unique_lock<std::mutex> ReadLock(pthis->m_read_mutex);

            if (!pthis->m_read_queue.empty())
            {
                request_ptr req = pthis->m_read_queue.front();
                pthis->m_read_queue.pop_front();

                ReadLock.unlock();

                LOG << "queue: before serve request has " << req->reference_count() << " references ";
                // assert(req->get_reference_count() > 1);
                dynamic_cast<serving_request*>(req.get())->serve();
                LOG << "queue: after serve request has " << req->reference_count() << " references ";
            }
            else
            {
                ReadLock.unlock();

                pthis->m_sem++;

                if (pthis->m_priority_op == READ)
                    write_phase = true;
            }

            if (pthis->m_priority_op == NONE || pthis->m_priority_op == WRITE)
                write_phase = true;
        }

        // terminate if it has been requested and queues are empty
        if (pthis->m_thread_state() == TERMINATING) {
            if ((pthis->m_sem--) == 0)
                break;
            else
                pthis->m_sem++;
        }
    }

    pthis->m_thread_state.set_to(TERMINATED);

#if STXXL_STD_THREADS && STXXL_MSVC >= 1700
    // Workaround for deadlock bug in Visual C++ Runtime 2012 and 2013, see
    // request_queue_impl_worker.cpp. -tb
    ExitThread(nullptr);
#else
    return nullptr;
#endif
}

} // namespace io
} // namespace thrill

/******************************************************************************/
