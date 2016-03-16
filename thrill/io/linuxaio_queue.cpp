/*******************************************************************************
 * thrill/io/linuxaio_queue.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2011 Johannes Singler <singler@kit.edu>
 * Copyright (C) 2014 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/linuxaio_queue.hpp>

#if THRILL_HAVE_LINUXAIO_FILE

#include <thrill/io/error_handling.hpp>
#include <thrill/io/linuxaio_request.hpp>

#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>

#ifndef THRILL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
#define THRILL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION 1
#endif

namespace thrill {
namespace io {

LinuxaioQueue::LinuxaioQueue(int desired_queue_length)
    : num_waiting_requests_(0), num_free_events_(0), num_posted_requests_(0),
      post_thread_state_(NOT_RUNNING), wait_thread_state_(NOT_RUNNING) {
    if (desired_queue_length == 0) {
        // default value, 64 entries per queue (i.e. usually per disk) should
        // be enough
        max_events_ = 64;
    }
    else
        max_events_ = desired_queue_length;

    // negotiate maximum number of simultaneous events with the OS
    context_ = 0;
    long result;
    while ((result = syscall(SYS_io_setup, max_events_, &context_)) == -1 &&
           errno == EAGAIN && max_events_ > 1)
    {
        max_events_ <<= 1;               // try with half as many events
    }
    if (result != 0) {
        THRILL_THROW_ERRNO(IoError, "linuxaio_queue::linuxaio_queue"
                           " io_setup() nr_events=" << max_events_);
    }

    for (int e = 0; e < max_events_; ++e)
        num_free_events_++;  // cannot set semaphore to value directly

    LOG1 << "Set up an linuxaio queue with " << max_events_ << " entries.";

    StartThread(post_async, static_cast<void*>(this), post_thread_, post_thread_state_);
    StartThread(wait_async, static_cast<void*>(this), wait_thread_, wait_thread_state_);
}

LinuxaioQueue::~LinuxaioQueue() {
    StopThread(post_thread_, post_thread_state_, num_waiting_requests_);
    StopThread(wait_thread_, wait_thread_state_, num_posted_requests_);
    syscall(SYS_io_destroy, context_);
}

void LinuxaioQueue::add_request(RequestPtr& req) {
    if (req.empty())
        THRILL_THROW_INVALID_ARGUMENT("Empty request submitted to disk_queue.");
    if (post_thread_state_() != RUNNING)
        LOG1 << "Request submitted to stopped queue.";
    if (!dynamic_cast<LinuxaioRequest*>(req.get()))
        LOG1 << "Non-LinuxAIO request submitted to LinuxAIO queue.";

    std::unique_lock<std::mutex> lock(waiting_mtx_);

    waiting_requests_.push_back(req);
    num_waiting_requests_++;
}

bool LinuxaioQueue::cancel_request(RequestPtr& req) {
    if (req.empty())
        THRILL_THROW_INVALID_ARGUMENT("Empty request canceled disk_queue.");
    if (post_thread_state_() != RUNNING)
        LOG1 << "Request canceled in stopped queue.";
    if (!dynamic_cast<LinuxaioRequest*>(req.get()))
        LOG1 << "Non-LinuxAIO request submitted to LinuxAIO queue.";

    Queue::iterator pos;
    {
        std::unique_lock<std::mutex> lock(waiting_mtx_);

        pos = std::find(waiting_requests_.begin(), waiting_requests_.end(), req);
        if (pos != waiting_requests_.end())
        {
            waiting_requests_.erase(pos);

            // polymorphic_downcast to linuxaio_request,
            // request is canceled, but was not yet posted.
            dynamic_cast<LinuxaioRequest*>(req.get())->completed(false, true);

            num_waiting_requests_--; // will never block
            return true;
        }
    }

    std::unique_lock<std::mutex> lock(posted_mtx_);

    pos = std::find(posted_requests_.begin(), posted_requests_.end(), req);
    if (pos != posted_requests_.end())
    {
        // polymorphic_downcast to linuxaio_request,
        bool canceled_io_operation = (dynamic_cast<LinuxaioRequest*>(req.get()))->cancel_aio();

        if (canceled_io_operation)
        {
            posted_requests_.erase(pos);

            // polymorphic_downcast to linuxaio_request,

            // request is canceled, already posted
            dynamic_cast<LinuxaioRequest*>(req.get())->completed(true, true);

            num_free_events_++;
            num_posted_requests_--; // will never block
            return true;
        }
    }

    return false;
}

// internal routines, run by the posting thread
void LinuxaioQueue::post_requests() {
    RequestPtr req;
    io_event* events = new io_event[max_events_];

    for ( ; ; ) // as long as thread is running
    {
        // might block until next request or message comes in
        int num_currently_waiting_requests = num_waiting_requests_--;

        // terminate if termination has been requested
        if (post_thread_state_() == TERMINATING && num_currently_waiting_requests == 0)
            break;

        std::unique_lock<std::mutex> lock(waiting_mtx_);
        if (!waiting_requests_.empty())
        {
            req = waiting_requests_.front();
            waiting_requests_.pop_front();
            lock.unlock();

            num_free_events_--; // might block because too many requests are posted

            // polymorphic_downcast
            while (!dynamic_cast<LinuxaioRequest*>(req.get())->post())
            {
                // post failed, so first handle events to make queues (more)
                // empty, then try again.

                // wait for at least one event to complete, no time limit
                long num_events = syscall(SYS_io_getevents, context_, 1, max_events_, events, nullptr);
                if (num_events < 0) {
                    THRILL_THROW_ERRNO(IoError, "linuxaio_queue::post_requests"
                                       " io_getevents() nr_events=" << num_events);
                }

                handle_events(events, num_events, false);
            }

            // request is finally posted

            {
                std::unique_lock<std::mutex> lock(posted_mtx_);
                posted_requests_.push_back(req);
                num_posted_requests_++;
            }
        }
        else
        {
            lock.unlock();

            // num_waiting_requests-- was premature, compensate for that
            num_waiting_requests_++;
        }
    }

    delete[] events;
}

void LinuxaioQueue::handle_events(io_event* events, long num_events, bool canceled) {
    for (int e = 0; e < num_events; ++e)
    {
        // size_t is as long as a pointer, and like this, we avoid an icpc warning
        RequestPtr* r = reinterpret_cast<RequestPtr*>(static_cast<size_t>(events[e].data));
        r->get()->completed(canceled);
        delete r;               // release auto_ptr reference
        num_free_events_++;
        num_posted_requests_--; // will never block
    }
}

// internal routines, run by the waiting thread
void LinuxaioQueue::wait_requests() {
    RequestPtr req;
    io_event* events = new io_event[max_events_];

    for ( ; ; ) // as long as thread is running
    {
        // might block until next request is posted or message comes in
        int num_currently_posted_requests = num_posted_requests_--;

        // terminate if termination has been requested
        if (wait_thread_state_() == TERMINATING && num_currently_posted_requests == 0)
            break;

        // wait for at least one of them to finish
        long num_events = syscall(SYS_io_getevents, context_, 1, max_events_, events, nullptr);
        if (num_events < 0) {
            THRILL_THROW_ERRNO(IoError, "linuxaio_queue::wait_requests"
                               " io_getevents() nr_events=" << max_events_);
        }

        num_posted_requests_++; // compensate for the one eaten prematurely above

        handle_events(events, num_events, false);
    }

    delete[] events;
}

void* LinuxaioQueue::post_async(void* arg) {
    (static_cast<LinuxaioQueue*>(arg))->post_requests();

    self_type* pthis = static_cast<self_type*>(arg);
    pthis->post_thread_state_.set_to(TERMINATED);

#if THRILL_STD_THREADS && THRILL_MSVC >= 1700
    // Workaround for deadlock bug in Visual C++ Runtime 2012 and 2013, see
    // request_queue_impl_worker.cpp. -tb
    ExitThread(nullptr);
#else
    return nullptr;
#endif
}

void* LinuxaioQueue::wait_async(void* arg) {
    (static_cast<LinuxaioQueue*>(arg))->wait_requests();

    self_type* pthis = static_cast<self_type*>(arg);
    pthis->wait_thread_state_.set_to(TERMINATED);

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

#endif // #if THRILL_HAVE_LINUXAIO_FILE

/******************************************************************************/
