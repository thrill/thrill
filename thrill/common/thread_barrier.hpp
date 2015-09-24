/*******************************************************************************
 * thrill/common/thread_barrier.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_THREAD_BARRIER_HEADER
#define THRILL_COMMON_THREAD_BARRIER_HEADER

#include <thrill/common/defines.hpp>

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace thrill {
namespace common {

/*!
 * Implements a cyclic barrier using mutex locking and condition variables that
 * can be used to synchronize threads.
 */
class ThreadBarrierLocking
{
public:
    /*!
     * Creates a new barrier that waits for n threads.
     *
     * \param n The number of threads to wait for.
     */
    explicit ThreadBarrierLocking(size_t thread_count)
        : thread_count_(thread_count) { }

    /*!
     * Waits for n threads to arrive.
     *
     * This method blocks and returns as soon as n threads are waiting inside
     * the method.
     */
    void Await() {
        std::unique_lock<std::mutex> lock(mutex_);

        size_t local_ = current_;
        counts_[local_]++;

        if (counts_[local_] < thread_count_) {
            while (counts_[local_] < thread_count_) {
                cv_.wait(lock);
            }
        }
        else {
            current_ = current_ ? 0 : 1;
            counts_[current_] = 0;
            cv_.notify_all();
        }
    }

protected:
    std::mutex mutex_;
    std::condition_variable cv_;

    //! number of threads
    const size_t thread_count_;

    //! two counters: switch between then every run.
    size_t counts_[2] = { 0, 0 };

    //! current counter used.
    size_t current_ = 0;
};

/*!
 * Implements a cyclic barrier using atomics and a spin lock that can be used to
 * synchronize threads.
 *
 * !!! This ThreadBarrier implementation was a lot slower in tests !!!
 *
 * Do not use it (unless your measurements show different results)! It remains
 * here for reference purposes.
 */
class ThreadBarrierSpinning
{
public:
    /*!
     * Creates a new barrier that waits for n threads.
     *
     * \param n The number of threads to wait for.
     */
    explicit ThreadBarrierSpinning(size_t thread_count)
        : thread_count_(thread_count) { }

    /*!
     * Waits for n threads to arrive.
     *
     * This method blocks and returns as soon as n threads are waiting inside
     * the method.
     */
    void Await() {
        // get unique synchronization step.
        size_t this_step = step_.load();

        if ((waiting_ += 1) == thread_count_) {
            // we are the last thread to Await() -> reset and increment step.
            waiting_.store(0);
            step_.fetch_add(1);
        }
        else {
            // spin lock awaiting the last thread to increment the step counter.
            while (step_.load() == this_step) { }
        }
    }

protected:
    //! number of threads
    const size_t thread_count_;

    //! number of threads in spin lock
    std::atomic<size_t> waiting_ { 0 };

    //! barrier synchronization iteration
    std::atomic<size_t> step_ { 0 };
};

// select thread barrier implementation.
using ThreadBarrier = ThreadBarrierLocking;

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_THREAD_BARRIER_HEADER

/******************************************************************************/
