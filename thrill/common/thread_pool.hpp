/*******************************************************************************
 * thrill/common/thread_pool.hpp
 *
 * A ThreadPool of std::threads to work on jobs
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_THREAD_POOL_HEADER
#define THRILL_COMMON_THREAD_POOL_HEADER

#include <thrill/common/delegate.hpp>

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

namespace thrill {
namespace common {

/*!
 * ThreadPool starts a fixed number p of std::threads which process Jobs that
 * are \ref Enqueue "enqueued" into a concurrent job queue. The jobs
 * themselves can enqueue more jobs that will be processed when a thread is
 * ready.
 *
 * The ThreadPool can either run until
 *
 * 1. all jobs are done AND all threads are idle, when called with
 * LoopUntilEmpty(), or
 *
 * 2. until Terminate() is called when run with LoopUntilTerminate().
 *
 * Jobs are plain std::function<void()> objects (actually: common::delegate),
 * hence the pool user must pass in ALL CONTEXT himself. The best method to pass
 * parameters to Jobs is to use lambda captures. Alternatively, old-school
 * objects implementing operator(), or std::binds can be used.
 *
 * The ThreadPool uses a condition variable to wait for new jobs and does not
 * remain busy waiting.
 *
 * Note that the threads in the pool start **before** the two loop functions are
 * called. In case of LoopUntilEmpty() the threads continue to be idle
 * afterwards, and can be reused, until the ThreadPool is destroyed.

\code
ThreadPool pool(4); // pool with 4 threads

int value = 0;
pool.Enqueue([&value]() {
  // increment value in another thread.
  ++value;
});

pool.LoopUntilEmpty();
\endcode

 * ## Synchronization Primitives
 *
 * Beyond threads from the ThreadPool, the framework contains two fast
 * synchronized queue containers:
 *
 * - \ref ConcurrentQueue
 * - \ref ConcurrentBoundedQueue.
 *
 * If the Intel Thread Building Blocks are available, then these use their
 * lock-free implementations, which are very fast, but do busy-waiting for
 * items. Otherwise, compatible replacements are used.
 *
 * The \ref ConcurrentQueue has no busy-waiting pop(), only a try_pop()
 * method. This should be preferred! The \ref ConcurrentBoundedQueue<T> has a
 * blocking pop(), but it probably does busy-waiting.
 */
class ThreadPool
{
public:
    using Job = delegate<void()>;

private:
    //! Deque of scheduled jobs.
    std::deque<Job> jobs_;

    //! Mutex used to access the queue of scheduled jobs.
    std::mutex mutex_;

    //! threads in pool
    std::vector<std::thread> threads_;

    //! Condition variable used to notify that a new job has been inserted in
    //! the queue.
    std::condition_variable cv_jobs_;
    //! Condition variable to signal when a jobs finishes.
    std::condition_variable cv_finished_;

    //! Counter for number of threads busy.
    std::atomic<size_t> busy_ = { 0 };
    //! Counter for total number of jobs executed
    std::atomic<size_t> done_ = { 0 };

    //! Flag whether to terminate
    std::atomic<bool> terminate_ = { false };

public:
    //! Construct running thread pool of num_threads
    explicit ThreadPool(
        size_t num_threads = std::thread::hardware_concurrency());

    //! non-copyable: delete copy-constructor
    ThreadPool(const ThreadPool&) = delete;
    //! non-copyable: delete assignment operator
    ThreadPool& operator = (const ThreadPool&) = delete;

    //! Stop processing jobs, terminate threads.
    ~ThreadPool();

    //! Enqueue a Job, the caller must pass in all context using captures.
    void Enqueue(Job&& job) {
        std::unique_lock<std::mutex> lock(mutex_);
        jobs_.emplace_back(std::move(job));
        cv_jobs_.notify_all();
    }

    //! Loop until no more jobs are in the queue AND all threads are idle. When
    //! this occurs, this method exits, however, the threads remain active.
    void LoopUntilEmpty();

    //! Loop until terminate flag was set.
    void LoopUntilTerminate();

    //! Terminate thread pool gracefully, wait until currently running jobs
    //! finish and then exit. This should be called from within one of the
    //! enqueue jobs or from an outside thread.
    void Terminate();

    //! Return number of jobs currently completed.
    size_t done() const {
        return done_;
    }

    //! Return number of threads in pool
    size_t size() const {
        return threads_.size();
    }

    //! Return thread handle to thread i
    std::thread& thread(size_t i) {
        assert(i < threads_.size());
        return threads_[i];
    }

private:
    //! Worker function, one per thread is started.
    void Worker();
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_THREAD_POOL_HEADER

/******************************************************************************/
