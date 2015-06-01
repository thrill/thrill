/*******************************************************************************
 * c7a/common/thread_pool.hpp
 *
 * A ThreadPool of std::threads to work on jobs
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_THREAD_POOL_HEADER
#define C7A_COMMON_THREAD_POOL_HEADER

#include <c7a/common/logger.hpp>

#include <atomic>
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
 * Jobs are plain std::function<void()> objects, hence the pool user must pass
 * in ALL CONTEXT himself.
 */
class ThreadPool
{
public:
    typedef std::function<void ()> Job;

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
    std::atomic<size_t> busy_;
    //! Counter for total number of jobs executed
    std::atomic<size_t> done_;

    //! Flag whether to terminate
    bool terminate_ = false;

public:
    //! Construct running thread pool of num_threads
    explicit ThreadPool(
        size_t num_threads = std::thread::hardware_concurrency())
        : threads_(num_threads),
          busy_(0), done_(0) {
        // immediately construct worker threads
        for (size_t i = 0; i < num_threads; ++i)
            threads_[i] = std::thread(&ThreadPool::Worker, this);
    }

    //! non-copyable: delete copy-constructor
    ThreadPool(const ThreadPool&) = delete;
    //! non-copyable: delete assignment operator
    ThreadPool& operator = (const ThreadPool&) = delete;

    //! Stop processing jobs, terminate threads.
    ~ThreadPool() {
        // set stop-condition
        std::unique_lock<std::mutex> lock(mutex_);
        terminate_ = true;
        cv_jobs_.notify_all();
        lock.unlock();

        // all threads terminate, then we're done
        for (size_t i = 0; i < threads_.size(); ++i)
            threads_[i].join();
    }

    //! Enqueue a Job, the caller must pass in all context using captures.
    void Enqueue(const Job& job) {
        std::unique_lock<std::mutex> lock(mutex_);
        jobs_.emplace_back(std::move(job));
        cv_jobs_.notify_all();
    }

    //! Loop until no more jobs are in the queue AND all threads are idle. When
    //! this occurs, this method exits, however, the threads remain active.
    void LoopUntilEmpty() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_finished_.wait(lock, [this]() {
                              return jobs_.empty() && (busy_ == 0);
                          });
    }

    //! Loop until terminate flag was set.
    void LoopUntilTerminate() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_finished_.wait(lock, [this]() {
                              return terminate_ && (busy_ == 0);
                          });
    }

    //! Terminate thread pool gracefully, wait until currently running jobs
    //! finish and then exit. This should be called from within one of the
    //! enqueue jobs or from an outside thread.
    void Terminate() {
        // flag termination
        terminate_ = true;
        // wake up all worker threads and let them terminate.
        cv_jobs_.notify_all();
        // notify LoopUntilTerminate in case all threads are idle.
        cv_finished_.notify_one();
    }

    //! Return number of jobs currently completed.
    size_t done() const {
        return done_;
    }

protected:
    //! Worker function, one per thread is started.
    void Worker() {
        while (true) {
            // wait for next job
            std::unique_lock<std::mutex> lock(mutex_);

            // wait on condition variable until job arrives, frees lock
            cv_jobs_.wait(lock, [this]() {
                              return terminate_ || !jobs_.empty();
                          });

            if (terminate_) break;

            if (!jobs_.empty()) {
                // got work. set busy.
                ++busy_;

                // pull job.
                Job job = jobs_.front();
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

                ++done_;
                --busy_;

                cv_finished_.notify_one();
            }
        }
    }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_THREAD_POOL_HEADER

/******************************************************************************/
