/*******************************************************************************
 * c7a/common/concurrent_bounded_queue.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_CONCURRENT_BOUNDED_QUEUE_HEADER
#define C7A_COMMON_CONCURRENT_BOUNDED_QUEUE_HEADER

#if HAVE_INTELTBB

#include <tbb/concurrent_queue.h>

#else // !HAVE_INTELTBB

#include <queue>
#include <mutex>
#include <condition_variable>

#endif // !HAVE_INTELTBB

namespace c7a {
namespace common {

#if HAVE_INTELTBB

template <typename T>
using concurrent_bounded_queue = tbb::concurrent_bounded_queue<T>;

#else   // !HAVE_INTELTBB

/*!
 * This is a queue, similar to std::queue and tbb::concurrent_bounded_queue,
 * except that it uses mutexes for synchronization. This implementation is only
 * here to be used if the Intel TBB is not available.
 *
 * Not all methods of tbb:concurrent_bounded_queue<> are available here, please
 * add them if you need them. However, NEVER add any other methods that you
 * might need.
 */
template <typename T>
class concurrent_bounded_queue
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

    //! condition variable signaled when an item arrives
    std::condition_variable cv_;

public:
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
};

#endif // !HAVE_INTELTBB

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_CONCURRENT_BOUNDED_QUEUE_HEADER

/******************************************************************************/
