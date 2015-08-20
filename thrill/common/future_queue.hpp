/*******************************************************************************
 * thrill/common/future_queue.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_FUTURE_QUEUE_HEADER
#define THRILL_COMMON_FUTURE_QUEUE_HEADER

#include <cassert>
#include <condition_variable>
#include <deque>
#include <mutex>

namespace thrill {
namespace common {

/*!
 * FutureQueue helps you deal with the nasty callbacks in the land of callback-hell.
 *
 * Handles the use-case where a callback is expected to be called once or multiple times.
 * If you expect only a single call, use \ref Future.
 *
 * One thread waits on this FutureQueue by calling \ref Wait() or \ref WaitForAll()
 * which returns directly if data is available and blocks otherwise until data
 * is available.
 * Another thread holds the callback to this FutureQueue which can be accquired
 * via \ref Callback() . The callback can be used to signal arrival of data
 * or signaling the end of stream.
 *
 * FutureQueue can currently only be consumed by a single thread.//TODO(ts) change that.
 */
template <typename T>
class FutureQueue
{
protected:
    //! Mutex for the condition variable
    std::mutex mutex_;

    //! For Notifications to the blocking thread
    std::condition_variable cv_;

    //! state that indicates whether the queue was closed
    //! Closed queues do not accept any callbacks
    bool closed_ = false;

    //! Stores the value if callback returned before next was invoked
    std::deque<T> elements_;

public:
    //! Returns the callback that feeds back to this future.
    //! Is Used to signal arrival of data \code (x, false)\endcode or to signal
    //! the end of the queue \code(dummy, true)\endcode. In the latter case
    //! the dummy value will be ignored.
    void Callback(T&& data, bool finished) {
        std::unique_lock<std::mutex> lock(mutex_);
        assert(!closed_);
        if (finished)
            closed_ = true;
        else
            elements_.emplace_back(std::move(data));
        cv_.notify_one();
    }

    //! Blocks until at least one element is available (returns true) or queue
    //! is closed (false).
    //! If the queue is closed this call is garantueed to be non-blocking.
    bool Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!elements_.empty())
            return true;
        if (!closed_) {
            cv_.wait(lock);
        }
        return !elements_.empty();
    }

    //! Blocks until all elements are available (returns true) or queue
    //! is closed without any elements (false).
    //! If queue is closed this call is garantueed to be non-blocking.
    bool WaitForAll() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [=]() { return closed_; });
        return !elements_.empty();
    }

    //! Indicates if queue is closed.
    //! Closed queues do not accept any elements.
    //! Closed queues are non-blocking for Wait calls.
    bool closed() {
        std::unique_lock<std::mutex> lock(mutex_);
        return closed_;
    }

    //! Returns the next element
    //! Undefined behaviour if Wait() returns false.
    T Next() {
        std::unique_lock<std::mutex> lock(mutex_);
        assert(!elements_.empty());

        T elem = std::move(elements_.front());
        elements_.pop_front();
        return std::move(elem);
    }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_FUTURE_QUEUE_HEADER

/******************************************************************************/
