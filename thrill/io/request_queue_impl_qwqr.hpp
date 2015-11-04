/*******************************************************************************
 * thrill/io/request_queue_impl_qwqr.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_REQUEST_QUEUE_IMPL_QWQR_HEADER
#define THRILL_IO_REQUEST_QUEUE_IMPL_QWQR_HEADER

#include <thrill/io/request_queue_impl_worker.hpp>

#include <list>
#include <mutex>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

//! Implementation of a local request queue having two queues, one for read and
//! one for write requests, thus having two threads. This is the default
//! implementation.
class request_queue_impl_qwqr : public request_queue_impl_worker
{
    static const bool debug = false;

private:
    using self = request_queue_impl_qwqr;
    using queue_type = std::list<request_ptr>;

    std::mutex m_write_mutex;
    std::mutex m_read_mutex;
    queue_type m_write_queue;
    queue_type m_read_queue;

    common::state<thread_state> m_thread_state;
    thread_type m_thread;
    common::semaphore m_sem;

    static const priority_op m_priority_op = WRITE;

    static void * worker(void* arg);

public:
    // \param n max number of requests simultaneously submitted to disk
    request_queue_impl_qwqr(int n = 1);

    // in a multi-threaded setup this does not work as intended
    // also there were race conditions possible
    // and actually an old value was never restored once a new one was set ...
    // so just disable it and all it's nice implications
    void set_priority_op(priority_op op) final {
        // _priority_op = op;
        common::THRILL_UNUSED(op);
    }
    void add_request(request_ptr& req) final;
    bool cancel_request(request_ptr& req) final;
    ~request_queue_impl_qwqr();
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_REQUEST_QUEUE_IMPL_QWQR_HEADER

/******************************************************************************/
