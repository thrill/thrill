/*******************************************************************************
 * c7a/common/thread_barrier.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_THREAD_BARRIER_HEADER
#define C7A_COMMON_THREAD_BARRIER_HEADER

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace c7a {
namespace common {

/**
 * Implements a cyclic barrier that can be shared between threads.
 */
class ThreadBarrier
{

private:
    std::mutex mutex_;
    std::condition_variable_any event_;
    const size_t thread_count_;
    size_t counts_[2] = { 0, 0 };
    size_t current_ = 0;

public:
    /**
     * Creates a new barrier that waits for n threads.
     *
     * \param n The number of threads to wait for.
     */
    explicit ThreadBarrier(size_t n)
        : thread_count_(n) { }

    /**
     * Waits for n threads to arrive.
     *
     * This method blocks and returns as soon as n threads are waiting inside
     * the method.
     */
    void Await() {
        std::atomic_thread_fence(std::memory_order_release);
        mutex_.lock();
        size_t local_ = current_;
        counts_[local_]++;

        if (counts_[local_] < thread_count_) {
            while (counts_[local_] < thread_count_) {
                event_.wait(mutex_);
            }
        }
        else {
            current_ = current_ ? 0 : 1;
            counts_[current_] = 0;
            event_.notify_all();
        }
        mutex_.unlock();
    }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_THREAD_BARRIER_HEADER

/******************************************************************************/
