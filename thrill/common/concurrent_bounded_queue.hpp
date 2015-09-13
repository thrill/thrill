/*******************************************************************************
 * thrill/common/concurrent_bounded_queue.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_CONCURRENT_BOUNDED_QUEUE_HEADER
#define THRILL_COMMON_CONCURRENT_BOUNDED_QUEUE_HEADER

#if THRILL_HAVE_INTELTBB

#include <tbb/concurrent_queue.h>

#endif // THRILL_HAVE_INTELTBB

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>

namespace thrill {
namespace common {

/*!
 * This is a queue, similar to std::queue and tbb::concurrent_bounded_queue,
 * except that it uses mutexes for synchronization. This implementation is only
 * here to be used if the Intel TBB is not available.
 *
 * Not all methods of tbb::concurrent_bounded_queue<> are available here, please
 * add them if you need them. However, NEVER add any other methods that you
 * might need.
 *
 * StyleGuide is violated, because signatures MUST match those in the TBB
 * version.
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

protected:
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
    void push(const T& source) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push(source);
        cv_.notify_one();
    }

    //! Pushes given element into the queue by utilizing element's move
    //! constructor
    void push(T&& elem) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push(std::move(elem));
        cv_.notify_one();
    }

    //! Pushes a new element into the queue. The element is constructed with
    //! given arguments.
    template <typename ... Arguments>
    void emplace(Arguments&& ... args) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.emplace(args ...);
        cv_.notify_one();
    }

    //! Returns: true if queue has no items; false otherwise.
    bool empty() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    //! Clears the queue.
    void clear() {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.clear();
    }

    //! If value is available, pops it from the queue, move it to destination,
    //! destroying the original position. Otherwise does nothing.
    bool try_pop(T& destination) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (queue_.empty())
            return false;

        destination = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    //! If value is available, pops it from the queue, move it to
    //! destination. If no item is in the queue, wait until there is one.
    void pop(T& destination) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [=]() { return !queue_.empty(); });
        destination = std::move(queue_.front());
        queue_.pop();
    }

    //! If value is available, pops it from the queue, move it to
    //! destination. If no item is in the queue, wait until there is one, or
    //! timeout and return false. NOTE: not available in TBB!
    template <class Rep, class Period>
    bool pop_for(T& destination,
                 const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!cv_.wait_for(lock, timeout, [=]() { return !queue_.empty(); })) {
            return false;
        }
        destination = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    //! return number of items available in the queue (tbb says "can return
    //! negative size", due to pending pop()s, but we ignore that here).
    size_t size() {
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.size();
    }
};

#if THRILL_HAVE_INTELTBB

template <typename T>
using ConcurrentBoundedQueue = tbb::concurrent_bounded_queue<T>;

#else   // !THRILL_HAVE_INTELTBB

template <typename T>
using ConcurrentBoundedQueue = OurConcurrentBoundedQueue<T>;

#endif // !THRILL_HAVE_INTELTBB

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_CONCURRENT_BOUNDED_QUEUE_HEADER

/******************************************************************************/
