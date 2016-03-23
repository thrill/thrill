/*******************************************************************************
 * thrill/common/thread_barrier.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_THREAD_BARRIER_HEADER
#define THRILL_COMMON_THREAD_BARRIER_HEADER

#include <thrill/common/defines.hpp>
#include <thrill/common/functional.hpp>

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
     */
    explicit ThreadBarrierLocking(size_t thread_count)
        : thread_count_(thread_count) { }

    /*!
     * Waits for n threads to arrive.
     *
     * This method blocks and returns as soon as n threads are waiting inside
     * the method.
     */
    template <typename Lambda = NoOperation<void> >
    void Await(Lambda lambda = Lambda()) {
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
            lambda();
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
 * This ThreadBarrier implementation was a lot faster in tests, but
 * ThreadSanitizer shows data races (probably due to the generation counter).
 */
class ThreadBarrierSpinning
{
public:
    /*!
     * Creates a new barrier that waits for n threads.
     */
    explicit ThreadBarrierSpinning(size_t thread_count)
        : thread_count_(thread_count) { }

    /*!
     * Waits for n threads to arrive.
     *
     * This method blocks and returns as soon as n threads are waiting inside
     * the method.
     */
    template <typename Lambda = NoOperation<void> >
    void Await(Lambda lambda = Lambda()) {
        // get synchronization generation step counter.
        size_t this_step = step_.load(std::memory_order_acquire);

        if (waiting_.fetch_add(1, std::memory_order_acq_rel) == thread_count_ - 1) {
            // we are the last thread to Await() -> reset and increment step.
            waiting_.store(0, std::memory_order_release);
            // step other generation counters.
            lambda();
            // the following statement releases all threads from busy waiting.
            step_.fetch_add(1, std::memory_order_acq_rel);
        }
        else {
            // spin lock awaiting the last thread to increment the step counter.
            while (step_.load(std::memory_order_relaxed) == this_step) { }
        }
    }

protected:
    //! number of threads
    const size_t thread_count_;

    //! number of threads in spin lock
    std::atomic<size_t> waiting_ { 0 };

    //! barrier synchronization generation
    std::atomic<size_t> step_ { 0 };
};

// select thread barrier implementation.
#if THRILL_HAVE_THREAD_SANITIZER || __APPLE__
using ThreadBarrier = ThreadBarrierLocking;
#else
using ThreadBarrier = ThreadBarrierSpinning;
#endif

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_THREAD_BARRIER_HEADER

/******************************************************************************/
