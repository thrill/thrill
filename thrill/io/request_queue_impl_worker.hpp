/*******************************************************************************
 * thrill/io/request_queue_impl_worker.hpp
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
#ifndef THRILL_IO_REQUEST_QUEUE_IMPL_WORKER_HEADER
#define THRILL_IO_REQUEST_QUEUE_IMPL_WORKER_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/semaphore.hpp>
#include <thrill/common/shared_state.hpp>
#include <thrill/io/request_queue.hpp>

#include <functional>
#include <thread>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

//! Implementation of request queue worker threads. Worker threads can be
//! started by start_thread and stopped with stop_thread. The queue state is
//! checked before termination and updated afterwards.
class RequestQueueImplWorker : public RequestQueue
{
protected:
    enum ThreadState { NOT_RUNNING, RUNNING, TERMINATING, TERMINATED };

    struct FileOffsetMatch
        : public std::binary_function<RequestPtr, RequestPtr, bool>
    {
        bool operator () (const RequestPtr& a, const RequestPtr& b) const {
            // matching file and offset are enough to cause problems
            return (a->offset() == b->offset()) &&
                   (a->file() == b->file());
        }
    };

protected:
    void StartThread(
        void* (*worker)(void*), void* arg,
        std::thread& t, common::SharedState<ThreadState>& s);

    void StopThread(
        std::thread& t, common::SharedState<ThreadState>& s,
        common::Semaphore& sem);
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_REQUEST_QUEUE_IMPL_WORKER_HEADER

/******************************************************************************/
