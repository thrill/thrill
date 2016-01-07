/*******************************************************************************
 * thrill/io/request_queue_impl_1q.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2005 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/config.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/io/error_handling.hpp>
#include <thrill/io/request_queue_impl_1q.hpp>
#include <thrill/io/serving_request.hpp>

#if STXXL_STD_THREADS && STXXL_MSVC >= 1700
 #include <windows.h>
#endif

#include <algorithm>
#include <functional>

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
        return (a->offset() == b->offset()) &&
               (a->file() == b->file());
    }
};

request_queue_impl_1q::request_queue_impl_1q(int n)
    : m_thread_state(NOT_RUNNING), m_sem(0) {
    common::THRILL_UNUSED(n);
    start_thread(worker, static_cast<void*>(this), m_thread, m_thread_state);
}

void request_queue_impl_1q::add_request(request_ptr& req) {
    if (req.empty())
        STXXL_THROW_INVALID_ARGUMENT("Empty request submitted to disk_queue.");
    if (m_thread_state() != RUNNING)
        STXXL_THROW_INVALID_ARGUMENT("Request submitted to not running queue.");
    if (!dynamic_cast<serving_request*>(req.get()))
        LOG1 << "Incompatible request submitted to running queue.";

#if STXXL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
    {
        std::unique_lock<std::mutex> Lock(m_queue_mutex);
        if (std::find_if(m_queue.begin(), m_queue.end(),
                         bind2nd(file_offset_match(), req))
            != m_queue.end())
        {
            LOG1 << "request submitted for a BID with a pending request";
        }
    }
#endif
    std::unique_lock<std::mutex> Lock(m_queue_mutex);
    m_queue.push_back(req);

    m_sem++;
}

bool request_queue_impl_1q::cancel_request(request_ptr& req) {
    if (req.empty())
        STXXL_THROW_INVALID_ARGUMENT("Empty request canceled disk_queue.");
    if (m_thread_state() != RUNNING)
        STXXL_THROW_INVALID_ARGUMENT("Request canceled to not running queue.");
    if (!dynamic_cast<serving_request*>(req.get()))
        LOG1 << "Incompatible request submitted to running queue.";

    bool was_still_in_queue = false;
    {
        std::unique_lock<std::mutex> Lock(m_queue_mutex);
        queue_type::iterator pos
            = std::find(m_queue.begin(), m_queue.end(), req);

        if (pos != m_queue.end())
        {
            m_queue.erase(pos);
            was_still_in_queue = true;
            Lock.unlock();
            m_sem--;
        }
    }

    return was_still_in_queue;
}

request_queue_impl_1q::~request_queue_impl_1q() {
    stop_thread(m_thread, m_thread_state, m_sem);
}

void* request_queue_impl_1q::worker(void* arg) {
    self* pthis = static_cast<self*>(arg);

    for ( ; ; )
    {
        pthis->m_sem--;

        {
            std::unique_lock<std::mutex> Lock(pthis->m_queue_mutex);
            if (!pthis->m_queue.empty())
            {
                request_ptr req = pthis->m_queue.front();
                pthis->m_queue.pop_front();

                Lock.unlock();

                // assert(req->nref() > 1);
                dynamic_cast<serving_request*>(req.get())->serve();
            }
            else
            {
                Lock.unlock();

                pthis->m_sem++;
            }
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
