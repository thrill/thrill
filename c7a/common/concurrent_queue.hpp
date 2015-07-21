/*******************************************************************************
 * c7a/common/concurrent_queue.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_CONCURRENT_QUEUE_HEADER
#define C7A_COMMON_CONCURRENT_QUEUE_HEADER

#if HAVE_INTELTBB

#include <tbb/concurrent_queue.h>

#endif // HAVE_INTELTBB

#include <mutex>
#include <queue>

namespace c7a {
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
template <typename T>
class OurConcurrentQueue
{
public:
    typedef T value_type;
    typedef T& reference;
    typedef const T& const_reference;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

protected:
    //! the actual data queue
    std::queue<T> queue_;

    //! the mutex to lock before accessing the queue
    mutable std::mutex mutex_;

public:
    //! Pushes a copy of source onto back of the queue.
    void push(const T& source) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push(source);
    }

    //! Pushes given element into the queue by utilizing element's move
    //! constructor
    void push(T&& elem) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.push(std::move(elem));
    }

    //! Pushes a new element into the queue. The element is constructed with
    //! given arguments.
    template <typename ... Arguments>
    void emplace(Arguments&& ... args) {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.emplace(args ...);
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
        queue_.pop();
        return true;
    }

    //! Clears the queue.
    void clear() {
        std::unique_lock<std::mutex> lock(mutex_);
        queue_.clear();
    }
};

#if HAVE_INTELTBB

template <typename T>
using ConcurrentQueue = tbb::concurrent_queue<T>;

#else   // !HAVE_INTELTBB

template <typename T>
using ConcurrentQueue = OurConcurrentQueue<T>;

#endif // !HAVE_INTELTBB

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_CONCURRENT_QUEUE_HEADER

/******************************************************************************/
