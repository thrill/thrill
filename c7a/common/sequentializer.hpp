/*******************************************************************************
 * c7a/common/sequentializer.hpp
 *
 * A single thread with a synchronized job queue, much the same as a thread pool
 * with one thread.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_SEQUENTIALIZER_HEADER
#define C7A_COMMON_SEQUENTIALIZER_HEADER

#include <c7a/common/logger.hpp>
#include <c7a/common/thread_pool.hpp>

namespace c7a {
namespace common {

/*!
 * Sequentializer is a single thread with a synchronized job queue that it
 * processes. Because there is only one thread the jobs in the queue are
 * sequentialized. This is very similar to a thread pool with just one thread,
 * however, as the Sequentializer usually always has one running job, we may use
 * a lock-free implementation of the job queue in future.
 *
 * For the time being, this actually is a ThreadPool(1).
 *
 * Jobs are plain std::function<void()> objects, hence the class user must pass
 * in ALL CONTEXT himself.
 */
class Sequentializer : public ThreadPool
{
public:
    using Job = ThreadPool::Job;

public:
    //! Construct running thread pool of num_threads
    Sequentializer()
        : ThreadPool(1)
    { }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_SEQUENTIALIZER_HEADER

/******************************************************************************/
