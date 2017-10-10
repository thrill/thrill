/*******************************************************************************
 * thrill/common/concurrent_bounded_queue.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_CONCURRENT_BOUNDED_QUEUE_HEADER
#define THRILL_COMMON_CONCURRENT_BOUNDED_QUEUE_HEADER

#include <concurrentqueue/blockingconcurrentqueue.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>

namespace thrill {
namespace common {

/*!
 * This is a synchronized queue, similar to std::queue and moodycamel's
 * BlockingConcurrentQueue, except that it uses mutexes for
 * synchronization. This implementation is for speed and interface testing
 * against a lock-free queue.
 */
template <typename T>
class OurConcurrentBoundedQueue
{
public:
    using value_type = T;
    using reference = T &;
    using const_reference = const T &;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

private:
    //! the actual data queue
    std::queue<T> queue_;

    //! the mutex to lock before accessing the queue
    mutable std::mutex mutex_;

    //! condition variable signaled when an item arrives
    std::condition_variable cv_;

public:
    //! default constructor
    OurConcurrentBoundedQueue() = default;

    //! move-constructor
    OurConcurrentBoundedQueue(OurConcurrentBoundedQueue&& other) {
        std::unique_lock<std::mutex> lock(other.mutex_);
        queue_ = std::move(other.queue_);
    }

    //! Pushes a copy of source onto back of the queue.
    void enqueue(const T& source) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push(source);
        cv_.notify_one();
    }

    //! Pushes given element into the queue by utilizing element's move
    //! constructor
    void enqueue(T&& elem) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push(std::move(elem));
        cv_.notify_one();
    }

    //! Returns: approximate number of items in queue.
    size_t size_approx() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.size();
    }

    //! If value is available, pops it from the queue, move it to destination,
    //! destroying the original position. Otherwise does nothing.
    bool try_dequeue(T& destination) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (queue_.empty())
            return false;

        destination = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    //! If value is available, pops it from the queue, move it to
    //! destination. If no item is in the queue, wait until there is one.
    void wait_dequeue(T& destination) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [=]() { return !queue_.empty(); });
        destination = std::move(queue_.front());
        queue_.pop();
    }

    //! If value is available, pops it from the queue, move it to
    //! destination. If no item is in the queue, wait until there is one, or
    //! timeout and return false.
    template <typename Rep, typename Period>
    bool wait_dequeue_timed(T& destination,
                            const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!cv_.wait_for(lock, timeout, [=]() { return !queue_.empty(); })) {
            return false;
        }
        destination = std::move(queue_.front());
        queue_.pop();
        return true;
    }
};

#if 0

template <typename T>
using ConcurrentBoundedQueue = OurConcurrentBoundedQueue<T>;

#else

template <typename T>
using ConcurrentBoundedQueue = moodycamel::BlockingConcurrentQueue<T>;

#endif

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_CONCURRENT_BOUNDED_QUEUE_HEADER

/******************************************************************************/
