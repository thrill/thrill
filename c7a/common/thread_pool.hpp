/*******************************************************************************
 * c7a/common/thread_pool.hpp
 *
 * A ThreadPool of std::threads to work on jobs
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_THREAD_POOL_HEADER
#define C7A_COMMON_THREAD_POOL_HEADER

#include <c7a/common/delegate.hpp>

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

namespace c7a {
namespace common {

/*!
 * ThreadPool is a fixed number of std::threads which process work from a
 * concurrent job queue. The pool can either run until a) all jobs are done AND
 * all threads are idle, or b) until a termination flag is set. The thread use
 * condition variable to wait for new jobs and do not remain busy waiting.
 *
 * Jobs are plain std::function<void()> objects (actually: our delegates), hence
 * the pool user must pass in ALL CONTEXT himself.
 */
class ThreadPool
{
public:
    using Job = delegate<void()>;

protected:
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
    std::thread & thread(size_t i) {
        assert(i < threads_.size());
        return threads_[i];
    }

protected:
    //! Worker function, one per thread is started.
    void Worker();
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_THREAD_POOL_HEADER

/******************************************************************************/
