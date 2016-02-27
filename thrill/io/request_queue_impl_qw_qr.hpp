/*******************************************************************************
 * thrill/io/request_queue_impl_qw_qr.hpp
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
#ifndef THRILL_IO_REQUEST_QUEUE_IMPL_QW_QR_HEADER
#define THRILL_IO_REQUEST_QUEUE_IMPL_QW_QR_HEADER

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
class RequestQueueImplQwQr : public RequestQueueImplWorker
{
    static constexpr bool debug = false;

private:
    using self = RequestQueueImplQwQr;
    using queue_type = std::list<RequestPtr>;

    std::mutex write_mutex_;
    std::mutex read_mutex_;
    queue_type write_queue_;
    queue_type read_queue_;

    common::state<thread_state> thread_state_;
    Thread thread_;
    common::semaphore sem_;

    static constexpr priority_op priority_op_ = WRITE;

    static void * worker(void* arg);

public:
    // \param n max number of requests simultaneously submitted to disk
    explicit RequestQueueImplQwQr(int n = 1);

    // in a multi-threaded setup this does not work as intended
    // also there were race conditions possible
    // and actually an old value was never restored once a new one was set ...
    // so just disable it and all it's nice implications
    void set_priority_op(priority_op op) final {
        // _priority_op = op;
        common::THRILL_UNUSED(op);
    }
    void add_request(RequestPtr& req) final;
    bool cancel_request(RequestPtr& req) final;
    ~RequestQueueImplQwQr();
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_REQUEST_QUEUE_IMPL_QW_QR_HEADER

/******************************************************************************/
