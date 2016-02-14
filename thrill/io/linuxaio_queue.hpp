/*******************************************************************************
 * thrill/io/linuxaio_queue.hpp
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

#pragma once
#ifndef THRILL_IO_LINUXAIO_QUEUE_HEADER
#define THRILL_IO_LINUXAIO_QUEUE_HEADER

#include <thrill/io/request_queue_impl_worker.hpp>

#if THRILL_HAVE_LINUXAIO_FILE

#include <linux/aio_abi.h>

#include <list>
#include <mutex>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

//! Queue for linuxaio_file(s)
//!
//! Only one queue exists in a program, i.e. it is a singleton.
class LinuxaioQueue final : public RequestQueueImplWorker
{
    friend class LinuxaioRequest;

    using self_type = LinuxaioQueue;

private:
    //! OS context
    aio_context_t context;

    //! storing linuxaio_request* would drop ownership
    using queue_type = std::list<RequestPtr>;

    // "waiting" request have submitted to this queue, but not yet to the OS,
    // those are "posted"
    std::mutex waiting_mtx, posted_mtx;
    queue_type waiting_requests, posted_requests;

    //! max number of OS requests
    int max_events;
    //! number of requests in waitings_requests
    common::semaphore num_waiting_requests, num_free_events, num_posted_requests;

    // two threads, one for posting, one for waiting
    Thread post_thread, wait_thread;
    common::state<thread_state> post_thread_state, wait_thread_state;

    // Why do we need two threads, one for posting, and one for waiting?  Is
    // one not enough?
    // 1. User call cannot io_submit directly, since this tends to take
    //    considerable time sometimes
    // 2. A single thread cannot wait for the user program to post requests
    //    and the OS to produce I/O completion events at the same time
    //    (IOCB_CMD_NOOP does not seem to help here either)

    static const priority_op _priority_op = WRITE;

    static void * post_async(void* arg);   // thread start callback
    static void * wait_async(void* arg);   // thread start callback
    void post_requests();
    void handle_events(io_event* events, long num_events, bool canceled);
    void wait_requests();
    void suspend();

    // needed by linuxaio_request
    aio_context_t get_io_context() { return context; }

public:
    //! Construct queue. Requests max number of requests simultaneously
    //! submitted to disk, 0 means as many as possible
    explicit LinuxaioQueue(int desired_queue_length = 0);

    void add_request(RequestPtr& req) final;
    bool cancel_request(RequestPtr& req) final;
    void complete_request(RequestPtr& req);
    ~LinuxaioQueue();
};

//! \}

} // namespace io
} // namespace thrill

#endif // #if THRILL_HAVE_LINUXAIO_FILE

#endif // !THRILL_IO_LINUXAIO_QUEUE_HEADER

/******************************************************************************/
