/*******************************************************************************
 * thrill/common/concurrent_queue.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_CONCURRENT_QUEUE_HEADER
#define THRILL_COMMON_CONCURRENT_QUEUE_HEADER

#include <concurrentqueue/concurrentqueue.h>

#include <atomic>
#include <deque>
#include <mutex>

namespace thrill {
namespace common {

/*!
 * This is a synchronized queue, similar to std::queue and moodycamel's
 * concurrent_queue, except that it uses mutexes for synchronization. This
 * implementation is for speed and interface testing against a lock-free queue.
 */
template <typename T, typename Allocator = std::allocator<T> >
class OurConcurrentQueue
{
public:
    using value_type = T;
    using reference = T &;
    using const_reference = const T &;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

private:
    //! the actual data queue
    std::deque<T, Allocator> queue_;

    //! the mutex to lock before accessing the queue
    mutable std::mutex mutex_;

public:
    //! Constructor
    explicit OurConcurrentQueue(const Allocator& alloc = Allocator())
        : queue_(alloc) { }

    //! Pushes a copy of source onto back of the queue.
    void enqueue(const T& source) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push_back(source);
    }

    //! Pushes given element into the queue by utilizing element's move
    //! constructor
    void enqueue(T&& elem) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push_back(std::move(elem));
    }

    //! Returns: approximate number of items in queue.
    size_t size_approx() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.size();
    }

    //! If value is available, pops it from the queue, assigns it to
    //! destination, and destroys the original value. Otherwise does nothing.
    bool try_dequeue(T& destination) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (queue_.empty())
            return false;

        destination = std::move(queue_.front());
        queue_.pop_front();
        return true;
    }
};

#if 0

template <typename T>
using ConcurrentQueue = OurConcurrentQueue<T>;

#else

template <typename T>
using ConcurrentQueue = moodycamel::ConcurrentQueue<T>;

#endif

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_CONCURRENT_QUEUE_HEADER

/******************************************************************************/
