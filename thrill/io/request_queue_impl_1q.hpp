/*******************************************************************************
 * thrill/io/request_queue_impl_1q.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008-2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_REQUEST_QUEUE_IMPL_1Q_HEADER
#define THRILL_IO_REQUEST_QUEUE_IMPL_1Q_HEADER

#include <thrill/io/request_queue_impl_worker.hpp>

#include <list>
#include <mutex>

namespace thrill {
namespace io {

//! \addtogroup io_layer_req
//! \{

//! Implementation of a local request queue having only one queue for both read
//! and write requests, thus having only one thread.
class RequestQueueImpl1Q : public RequestQueueImplWorker
{
private:
    using Queue = std::list<RequestPtr, mem::GPoolAllocator<RequestPtr> >;

    std::mutex queue_mutex_;
    Queue queue_;

    common::SharedState<ThreadState> thread_state_;
    std::thread thread_;
    common::Semaphore sem_;

    static constexpr PriorityOp priority_op_ = WRITE;

    static void * worker(void* arg);

public:
    // \param n max number of requests simultaneously submitted to disk
    explicit RequestQueueImpl1Q(int n = 1);

    // in a multi-threaded setup this does not work as intended
    // also there were race conditions possible
    // and actually an old value was never restored once a new one was set ...
    // so just disable it and all it's nice implications
    void SetPriorityOp(PriorityOp op) final {
        // _priority_op = op;
        tlx::unused(op);
    }
    void AddRequest(RequestPtr& req) final;
    bool CancelRequest(Request* req) final;
    ~RequestQueueImpl1Q();
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_REQUEST_QUEUE_IMPL_1Q_HEADER

/******************************************************************************/
