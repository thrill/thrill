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

#if STXXL_HAVE_LINUXAIO_FILE

#include "error_handling.hpp"
#include <thrill/io/linuxaio_queue.hpp>
#include <thrill/io/linuxaio_request.hpp>

#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>

#ifndef STXXL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
#define STXXL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION 1
#endif

namespace thrill {
namespace io {

linuxaio_queue::linuxaio_queue(int desired_queue_length)
    : num_waiting_requests(0), num_free_events(0), num_posted_requests(0),
      post_thread_state(NOT_RUNNING), wait_thread_state(NOT_RUNNING) {
    if (desired_queue_length == 0) {
        // default value, 64 entries per queue (i.e. usually per disk) should
        // be enough
        max_events = 64;
    }
    else
        max_events = desired_queue_length;

    // negotiate maximum number of simultaneous events with the OS
    context = 0;
    long result;
    while ((result = syscall(SYS_io_setup, max_events, &context)) == -1 &&
           errno == EAGAIN && max_events > 1)
    {
        max_events <<= 1;               // try with half as many events
    }
    if (result != 0) {
        STXXL_THROW_ERRNO(io_error, "linuxaio_queue::linuxaio_queue"
                          " io_setup() nr_events=" << max_events);
    }

    for (int e = 0; e < max_events; ++e)
        num_free_events++;  // cannot set semaphore to value directly

    LOG1 << "Set up an linuxaio queue with " << max_events << " entries.";

    start_thread(post_async, static_cast<void*>(this), post_thread, post_thread_state);
    start_thread(wait_async, static_cast<void*>(this), wait_thread, wait_thread_state);
}

linuxaio_queue::~linuxaio_queue() {
    stop_thread(post_thread, post_thread_state, num_waiting_requests);
    stop_thread(wait_thread, wait_thread_state, num_posted_requests);
    syscall(SYS_io_destroy, context);
}

void linuxaio_queue::add_request(request_ptr& req) {
    if (req.empty())
        STXXL_THROW_INVALID_ARGUMENT("Empty request submitted to disk_queue.");
    if (post_thread_state() != RUNNING)
        LOG1 << "Request submitted to stopped queue.";
    if (!dynamic_cast<linuxaio_request*>(req.get()))
        LOG1 << "Non-LinuxAIO request submitted to LinuxAIO queue.";

    std::unique_lock<std::mutex> lock(waiting_mtx);

    waiting_requests.push_back(req);
    num_waiting_requests++;
}

bool linuxaio_queue::cancel_request(request_ptr& req) {
    if (req.empty())
        STXXL_THROW_INVALID_ARGUMENT("Empty request canceled disk_queue.");
    if (post_thread_state() != RUNNING)
        LOG1 << "Request canceled in stopped queue.";
    if (!dynamic_cast<linuxaio_request*>(req.get()))
        LOG1 << "Non-LinuxAIO request submitted to LinuxAIO queue.";

    queue_type::iterator pos;
    {
        std::unique_lock<std::mutex> lock(waiting_mtx);

        pos = std::find(waiting_requests.begin(), waiting_requests.end(), req);
        if (pos != waiting_requests.end())
        {
            waiting_requests.erase(pos);

            // polymorphic_downcast to linuxaio_request,
            // request is canceled, but was not yet posted.
            dynamic_cast<linuxaio_request*>(pos->get())->completed(false, true);

            num_waiting_requests--; // will never block
            return true;
        }
    }

    std::unique_lock<std::mutex> lock(posted_mtx);

    pos = std::find(posted_requests.begin(), posted_requests.end(), req);
    if (pos != posted_requests.end())
    {
        // polymorphic_downcast to linuxaio_request,
        bool canceled_io_operation = (dynamic_cast<linuxaio_request*>(req.get()))->cancel_aio();

        if (canceled_io_operation)
        {
            posted_requests.erase(pos);

            // polymorphic_downcast to linuxaio_request,

            // request is canceled, already posted
            dynamic_cast<linuxaio_request*>(pos->get())->completed(true, true);

            num_free_events++;
            num_posted_requests--; // will never block
            return true;
        }
    }

    return false;
}

// internal routines, run by the posting thread
void linuxaio_queue::post_requests() {
    request_ptr req;
    io_event* events = new io_event[max_events];

    for ( ; ; ) // as long as thread is running
    {
        // might block until next request or message comes in
        int num_currently_waiting_requests = num_waiting_requests--;

        // terminate if termination has been requested
        if (post_thread_state() == TERMINATING && num_currently_waiting_requests == 0)
            break;

        std::unique_lock<std::mutex> lock(waiting_mtx);
        if (!waiting_requests.empty())
        {
            req = waiting_requests.front();
            waiting_requests.pop_front();
            lock.unlock();

            num_free_events--; // might block because too many requests are posted

            // polymorphic_downcast
            while (!dynamic_cast<linuxaio_request*>(req.get())->post())
            {
                // post failed, so first handle events to make queues (more)
                // empty, then try again.

                // wait for at least one event to complete, no time limit
                long num_events = syscall(SYS_io_getevents, context, 1, max_events, events, nullptr);
                if (num_events < 0) {
                    STXXL_THROW_ERRNO(io_error, "linuxaio_queue::post_requests"
                                      " io_getevents() nr_events=" << num_events);
                }

                handle_events(events, num_events, false);
            }

            // request is finally posted

            {
                std::unique_lock<std::mutex> lock(posted_mtx);
                posted_requests.push_back(req);
                num_posted_requests++;
            }
        }
        else
        {
            lock.unlock();

            // num_waiting_requests-- was premature, compensate for that
            num_waiting_requests++;
        }
    }

    delete[] events;
}

void linuxaio_queue::handle_events(io_event* events, long num_events, bool canceled) {
    for (int e = 0; e < num_events; ++e)
    {
        // size_t is as long as a pointer, and like this, we avoid an icpc warning
        request_ptr* r = reinterpret_cast<request_ptr*>(static_cast<size_t>(events[e].data));
        r->get()->completed(canceled);
        delete r;              // release auto_ptr reference
        num_free_events++;
        num_posted_requests--; // will never block
    }
}

// internal routines, run by the waiting thread
void linuxaio_queue::wait_requests() {
    request_ptr req;
    io_event* events = new io_event[max_events];

    for ( ; ; ) // as long as thread is running
    {
        // might block until next request is posted or message comes in
        int num_currently_posted_requests = num_posted_requests--;

        // terminate if termination has been requested
        if (wait_thread_state() == TERMINATING && num_currently_posted_requests == 0)
            break;

        // wait for at least one of them to finish
        long num_events = syscall(SYS_io_getevents, context, 1, max_events, events, nullptr);
        if (num_events < 0) {
            STXXL_THROW_ERRNO(io_error, "linuxaio_queue::wait_requests"
                              " io_getevents() nr_events=" << max_events);
        }

        num_posted_requests++; // compensate for the one eaten prematurely above

        handle_events(events, num_events, false);
    }

    delete[] events;
}

void* linuxaio_queue::post_async(void* arg) {
    (static_cast<linuxaio_queue*>(arg))->post_requests();

    self_type* pthis = static_cast<self_type*>(arg);
    pthis->post_thread_state.set_to(TERMINATED);

#if STXXL_STD_THREADS && STXXL_MSVC >= 1700
    // Workaround for deadlock bug in Visual C++ Runtime 2012 and 2013, see
    // request_queue_impl_worker.cpp. -tb
    ExitThread(nullptr);
#else
    return nullptr;
#endif
}

void* linuxaio_queue::wait_async(void* arg) {
    (static_cast<linuxaio_queue*>(arg))->wait_requests();

    self_type* pthis = static_cast<self_type*>(arg);
    pthis->wait_thread_state.set_to(TERMINATED);

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

#endif // #if STXXL_HAVE_LINUXAIO_FILE

/******************************************************************************/
