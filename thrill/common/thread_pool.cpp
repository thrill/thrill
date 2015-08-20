/*******************************************************************************
 * thrill/common/thread_pool.cpp
 *
 * A ThreadPool of std::threads to work on jobs
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/common/thread_pool.hpp>

namespace thrill {
namespace common {

//! Construct running thread pool of num_threads
ThreadPool::ThreadPool(size_t num_threads)
    : threads_(num_threads) {
    // immediately construct worker threads
    for (size_t i = 0; i < num_threads; ++i)
        threads_[i] = std::thread(&ThreadPool::Worker, this);
}

//! Stop processing jobs, terminate threads.
ThreadPool::~ThreadPool() {
    std::unique_lock<std::mutex> lock(mutex_);
    // set stop-condition
    terminate_ = true;
    cv_jobs_.notify_all();
    lock.unlock();

    // all threads terminate, then we're done
    for (size_t i = 0; i < threads_.size(); ++i)
        threads_[i].join();
}

//! Loop until no more jobs are in the queue AND all threads are idle. When
//! this occurs, this method exits, however, the threads remain active.
void ThreadPool::LoopUntilEmpty() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_finished_.wait(
        lock, [this]() { return jobs_.empty() && (busy_ == 0); });
}

//! Loop until terminate flag was set.
void ThreadPool::LoopUntilTerminate() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_finished_.wait(
        lock, [this]() { return terminate_ && (busy_ == 0); });
}

//! Terminate thread pool gracefully, wait until currently running jobs
//! finish and then exit. This should be called from within one of the
//! enqueue jobs or from an outside thread.
void ThreadPool::Terminate() {
    std::unique_lock<std::mutex> lock(mutex_);
    // flag termination
    terminate_ = true;
    // wake up all worker threads and let them terminate.
    cv_jobs_.notify_all();
    // notify LoopUntilTerminate in case all threads are idle.
    cv_finished_.notify_one();
    lock.unlock();
}

//! Worker function, one per thread is started.
void ThreadPool::Worker() {
    while (true) {
        // wait for next job
        std::unique_lock<std::mutex> lock(mutex_);

        // wait on condition variable until job arrives, frees lock
        cv_jobs_.wait(
            lock, [this]() { return terminate_ || !jobs_.empty(); });

        if (terminate_) break;

        if (!jobs_.empty()) {
            // got work. set busy.
            ++busy_;

            {
                // pull job.
                Job job = std::move(jobs_.front());
                jobs_.pop_front();

                // release lock.
                lock.unlock();

                // execute job.
                try {
                    job();
                }
                catch (std::exception& e) {
                    LOG1 << "EXCEPTION: " << e.what();
                }
            }

            ++done_;
            --busy_;

            // relock mutex before signaling condition.
            lock.lock();
            cv_finished_.notify_one();
        }
    }
}

} // namespace common
} // namespace thrill

/******************************************************************************/
