/*******************************************************************************
 * thrill/common/concurrent_queue.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_CONCURRENT_QUEUE_HEADER
#define THRILL_COMMON_CONCURRENT_QUEUE_HEADER

#if THRILL_HAVE_INTELTBB

#include <tbb/concurrent_queue.h>

#endif // THRILL_HAVE_INTELTBB

#include <atomic>
#include <deque>
#include <mutex>

namespace thrill {
namespace common {

/*!
 * This is a queue, similar to std::queue and tbb::concurrent_queue, except that
 * it uses mutexes for synchronization. This implementation is only here to be
 * used if the Intel TBB is not available.
 *
 * Not all methods of tbb:concurrent_queue<> are available here, please add them
 * if you need them. However, NEVER add any other methods that you might need.
 *
 * StyleGuide is violated, because signatures MUST match those in the TBB
 * version.
 */
template <typename T, typename Allocator>
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
    void push(const T& source) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push_back(source);
    }

    //! Pushes given element into the queue by utilizing element's move
    //! constructor
    void push(T&& elem) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push_back(std::move(elem));
    }

    //! Pushes a new element into the queue. The element is constructed with
    //! given arguments.
    template <typename ... Arguments>
    void emplace(Arguments&& ... args) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.emplace_back(std::forward<Arguments>(args) ...);
    }

    //! Returns: true if queue has no items; false otherwise.
    bool empty() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    //! If value is available, pops it from the queue, assigns it to
    //! destination, and destroys the original value. Otherwise does nothing.
    bool try_pop(T& destination) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (queue_.empty())
            return false;

        destination = std::move(queue_.front());
        queue_.pop_front();
        return true;
    }

    //! Clears the queue.
    void clear() {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.clear();
    }
};

#if THRILL_HAVE_INTELTBB

template <typename T, typename Allocator>
using ConcurrentQueue = tbb::concurrent_queue<T, Allocator>;

#else   // !THRILL_HAVE_INTELTBB

template <typename T, typename Allocator>
using ConcurrentQueue = OurConcurrentQueue<T, Allocator>;

#endif // !THRILL_HAVE_INTELTBB

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_CONCURRENT_QUEUE_HEADER

/******************************************************************************/
