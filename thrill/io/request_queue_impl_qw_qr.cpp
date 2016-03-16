/*******************************************************************************
 * thrill/io/request_queue_impl_qw_qr.cpp
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

#include <thrill/common/logger.hpp>
#include <thrill/io/error_handling.hpp>
#include <thrill/io/request_queue_impl_qw_qr.hpp>
#include <thrill/io/serving_request.hpp>

#if THRILL_STD_THREADS && THRILL_MSVC >= 1700
 #include <windows.h>
#endif

#include <algorithm>
#include <functional>

#ifndef THRILL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
#define THRILL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION 1
#endif

namespace thrill {
namespace io {

struct file_offset_match : public std::binary_function<RequestPtr, RequestPtr, bool>
{
    bool operator () (
        const RequestPtr& a,
        const RequestPtr& b) const {
        // matching file and offset are enough to cause problems
        return (a->offset() == b->offset()) &&
               (a->file() == b->file());
    }
};

RequestQueueImplQwQr::RequestQueueImplQwQr(int n)
    : thread_state_(NOT_RUNNING), sem_(0) {
    common::THRILL_UNUSED(n);
    start_thread(worker, static_cast<void*>(this), thread_, thread_state_);
}

void RequestQueueImplQwQr::add_request(RequestPtr& req) {
    if (req.empty())
        THRILL_THROW_INVALID_ARGUMENT("Empty request submitted to disk_queue.");
    if (thread_state_() != RUNNING)
        THRILL_THROW_INVALID_ARGUMENT("Request submitted to not running queue.");
    if (!dynamic_cast<ServingRequest*>(req.get()))
        LOG1 << "Incompatible request submitted to running queue.";

    if (req.get()->type() == Request::READ)
    {
#if THRILL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
        {
            std::unique_lock<std::mutex> Lock(write_mutex_);
            if (std::find_if(write_queue_.begin(), write_queue_.end(),
                             bind2nd(file_offset_match(), req))
                != write_queue_.end())
            {
                LOG1 << "READ request submitted for a BID with a pending WRITE request";
            }
        }
#endif
        std::unique_lock<std::mutex> Lock(read_mutex_);
        read_queue_.push_back(req);
    }
    else
    {
#if THRILL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
        {
            std::unique_lock<std::mutex> Lock(read_mutex_);
            if (std::find_if(read_queue_.begin(), read_queue_.end(),
                             bind2nd(file_offset_match(), req))
                != read_queue_.end())
            {
                LOG1 << "WRITE request submitted for a BID with a pending READ request";
            }
        }
#endif
        std::unique_lock<std::mutex> Lock(write_mutex_);
        write_queue_.push_back(req);
    }

    sem_++;
}

bool RequestQueueImplQwQr::cancel_request(RequestPtr& req) {
    if (req.empty())
        THRILL_THROW_INVALID_ARGUMENT("Empty request canceled disk_queue.");
    if (thread_state_() != RUNNING)
        THRILL_THROW_INVALID_ARGUMENT("Request canceled to not running queue.");
    if (!dynamic_cast<ServingRequest*>(req.get()))
        LOG1 << "Incompatible request submitted to running queue.";

    bool was_still_in_queue = false;
    if (req.get()->type() == Request::READ)
    {
        std::unique_lock<std::mutex> Lock(read_mutex_);
        Queue::iterator pos
            = std::find(read_queue_.begin(), read_queue_.end(), req);
        if (pos != read_queue_.end())
        {
            read_queue_.erase(pos);
            was_still_in_queue = true;
            Lock.unlock();
            sem_--;
        }
    }
    else
    {
        std::unique_lock<std::mutex> Lock(write_mutex_);
        Queue::iterator pos
            = std::find(write_queue_.begin(), write_queue_.end(), req);
        if (pos != write_queue_.end())
        {
            write_queue_.erase(pos);
            was_still_in_queue = true;
            Lock.unlock();
            sem_--;
        }
    }

    return was_still_in_queue;
}

RequestQueueImplQwQr::~RequestQueueImplQwQr() {
    stop_thread(thread_, thread_state_, sem_);
}

void* RequestQueueImplQwQr::worker(void* arg) {
    RequestQueueImplQwQr* pthis = static_cast<RequestQueueImplQwQr*>(arg);

    bool write_phase = true;
    for ( ; ; )
    {
        pthis->sem_--;

        if (write_phase)
        {
            std::unique_lock<std::mutex> WriteLock(pthis->write_mutex_);
            if (!pthis->write_queue_.empty())
            {
                RequestPtr req = pthis->write_queue_.front();
                pthis->write_queue_.pop_front();

                WriteLock.unlock();

                // assert(req->get_reference_count()) > 1);
                dynamic_cast<ServingRequest*>(req.get())->serve();
            }
            else
            {
                WriteLock.unlock();

                pthis->sem_++;

                if (pthis->priority_op_ == WRITE)
                    write_phase = false;
            }

            if (pthis->priority_op_ == NONE || pthis->priority_op_ == READ)
                write_phase = false;
        }
        else
        {
            std::unique_lock<std::mutex> ReadLock(pthis->read_mutex_);

            if (!pthis->read_queue_.empty())
            {
                RequestPtr req = pthis->read_queue_.front();
                pthis->read_queue_.pop_front();

                ReadLock.unlock();

                LOG << "queue: before serve request has " << req->reference_count() << " references ";
                // assert(req->get_reference_count() > 1);
                dynamic_cast<ServingRequest*>(req.get())->serve();
                LOG << "queue: after serve request has " << req->reference_count() << " references ";
            }
            else
            {
                ReadLock.unlock();

                pthis->sem_++;

                if (pthis->priority_op_ == READ)
                    write_phase = true;
            }

            if (pthis->priority_op_ == NONE || pthis->priority_op_ == WRITE)
                write_phase = true;
        }

        // terminate if it has been requested and queues are empty
        if (pthis->thread_state_() == TERMINATING) {
            if ((pthis->sem_--) == 0)
                break;
            else
                pthis->sem_++;
        }
    }

    pthis->thread_state_.set_to(TERMINATED);

#if THRILL_STD_THREADS && THRILL_MSVC >= 1700
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
