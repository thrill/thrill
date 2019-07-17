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

#include <thrill/common/atomic_movable.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace thrill {
namespace common {

/*!
 * Implements a thread barrier using atomics and a spin lock that can be used to
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

    ~ThreadBarrierSpinning() {
        LOG0 << "ThreadBarrierSpinning() needed "
             << wait_time_.load() << " us for " << thread_count_ << " threads "
             << " = "
             << wait_time_.load() / static_cast<double>(thread_count_) / 1e6
             << " us avg";
    }

    /*!
     * Waits for n threads to arrive. When they have arrive, execute lambda on
     * the one thread, which arrived last. After lambda, step the generation
     * counter.
     *
     * This method blocks and returns as soon as n threads are waiting inside
     * the method.
     */
    template <typename Lambda = NoOperation<void> >
    void wait(Lambda lambda = Lambda()) {
        // get synchronization generation step counter.
        size_t this_step = step_.load(std::memory_order_acquire);

        if (waiting_.fetch_add(1, std::memory_order_acq_rel) == thread_count_ - 1) {
            // we are the last thread to wait() -> reset and increment step.
            waiting_.store(0, std::memory_order_release);
            // step other generation counters.
            lambda();
            // the following statement releases all threads from busy waiting.
            step_.fetch_add(1, std::memory_order_acq_rel);
        }
        else {
            // FakeStatsTimerStart timer;
            // spin lock awaiting the last thread to increment the step counter.
            while (step_.load(std::memory_order_acquire) == this_step) {
                // std::this_thread::yield();
            }
            // wait_time_ += timer.Microseconds();
        }
    }

    //! Return generation step counter
    size_t step() const {
        return step_.load(std::memory_order_acquire);
    }

    //! Return next generation step counter
    size_t next_step() const {
        return step_ ? 0 : 1;
    }

protected:
    //! number of threads
    const size_t thread_count_;

    //! number of threads in spin lock
    std::atomic<size_t> waiting_ { 0 };

    //! barrier synchronization generation
    std::atomic<size_t> step_ { 0 };

    AtomicMovable<uint64_t> wait_time_ { 0 };
};

// select thread barrier implementation.
#if THRILL_HAVE_THREAD_SANITIZER
using ThreadBarrier = tlx::ThreadBarrierMutex;
#else
using ThreadBarrier = ThreadBarrierSpinning;
#endif

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_THREAD_BARRIER_HEADER

/******************************************************************************/
