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

RequestQueueImpl1Q::RequestQueueImpl1Q(int n)
    : thread_state_(NOT_RUNNING), sem_(0) {
    common::THRILL_UNUSED(n);
    start_thread(worker, static_cast<void*>(this), thread_, thread_state_);
}

void RequestQueueImpl1Q::add_request(RequestPtr& req) {
    if (req.empty())
        THRILL_THROW_INVALID_ARGUMENT("Empty request submitted to disk_queue.");
    if (thread_state_() != RUNNING)
        THRILL_THROW_INVALID_ARGUMENT("Request submitted to not running queue.");
    if (!dynamic_cast<ServingRequest*>(req.get()))
        LOG1 << "Incompatible request submitted to running queue.";

#if THRILL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
    {
        std::unique_lock<std::mutex> Lock(queue_mutex_);
        if (std::find_if(queue_.begin(), queue_.end(),
                         bind2nd(file_offset_match(), req))
            != queue_.end())
        {
            LOG1 << "request submitted for a BID with a pending request";
        }
    }
#endif
    std::unique_lock<std::mutex> Lock(queue_mutex_);
    queue_.push_back(req);

    sem_++;
}

bool RequestQueueImpl1Q::cancel_request(RequestPtr& req) {
    if (req.empty())
        THRILL_THROW_INVALID_ARGUMENT("Empty request canceled disk_queue.");
    if (thread_state_() != RUNNING)
        THRILL_THROW_INVALID_ARGUMENT("Request canceled to not running queue.");
    if (!dynamic_cast<ServingRequest*>(req.get()))
        LOG1 << "Incompatible request submitted to running queue.";

    bool was_still_in_queue = false;
    {
        std::unique_lock<std::mutex> Lock(queue_mutex_);
        Queue::iterator pos
            = std::find(queue_.begin(), queue_.end(), req);

        if (pos != queue_.end())
        {
            queue_.erase(pos);
            was_still_in_queue = true;
            Lock.unlock();
            sem_--;
        }
    }

    return was_still_in_queue;
}

RequestQueueImpl1Q::~RequestQueueImpl1Q() {
    stop_thread(thread_, thread_state_, sem_);
}

void* RequestQueueImpl1Q::worker(void* arg) {
    RequestQueueImpl1Q* pthis = static_cast<RequestQueueImpl1Q*>(arg);

    for ( ; ; )
    {
        pthis->sem_--;

        {
            std::unique_lock<std::mutex> Lock(pthis->queue_mutex_);
            if (!pthis->queue_.empty())
            {
                RequestPtr req = pthis->queue_.front();
                pthis->queue_.pop_front();

                Lock.unlock();

                // assert(req->nref() > 1);
                dynamic_cast<ServingRequest*>(req.get())->serve();
            }
            else
            {
                Lock.unlock();

                pthis->sem_++;
            }
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
