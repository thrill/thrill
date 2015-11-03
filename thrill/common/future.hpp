/*******************************************************************************
 * thrill/common/future.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_FUTURE_HEADER
#define THRILL_COMMON_FUTURE_HEADER

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <mutex>
#include <tuple>

namespace thrill {
namespace common {

/*!
 * Future helps you deal with the nasty callbacks in the land of callback-hell
 * by waiting for callbacks to occur and storing their data for you to Wait().
 *
 * Handles the use-case where a callback is expected to be called exactly once!
 * If you expect multiple  calls use \ref FutureQueue
 *
 * Future can currently only be consumed by a single thread. //TODO(ts) change that.
 *
 */
template <typename T>
class Future
{
private:
    //! Mutex for the condition variable
    std::mutex mutex_;

    //! For notifications to the blocking thread
    std::condition_variable cv_;

    //! Indicates if promise was fulfilled.
    std::atomic<bool> triggered_ { false };

    //! state that indicates whether get was already called
    std::atomic<bool> finished_ { false };

    //! Stores the value if callback returned before next was invoked
    T value_;

public:
    ~Future() {
        std::unique_lock<std::mutex> lock(mutex_);
        // lock the Future before freeing it.
    }

    //! Fills future with the given data and wakes up waiting parties
    void operator << (T&& data) {
        std::unique_lock<std::mutex> lock(mutex_);
        value_ = std::move(data);
        triggered_ = true;
        cv_.notify_all();
    }

    //! Blocks until value is available and returns it
    T && Wait() {
        assert(!finished_); // prevent multiple calls to Wait()
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return triggered_.load(); });
        finished_ = true;
        return std::move(value_);
    }

    //! Returns value, must have gotten it with Wait()
    T && Get() {
        assert(finished_);
        return std::move(value_);
    }

    //! Returns true when the future was fulfilled.
    bool Test() const noexcept {
        return triggered_;
    }

    //! Indicates if get was invoked and returned
    //! Can be used at the end of a job to see if outstanding
    //! futures were not called.
    bool is_finished() const noexcept {
        return finished_;
    }
};

/*!
 * Future helps you deal with the nasty callbacks in the land of callback-hell
 * by waiting for callbacks to occur and storing their data for you to
 * Wait(). This is the variadic parameter variants, which will store any number
 * of parameters given by the callback in a tuple.
 *
 * Handles the use-case where a callback is expected to be called exactly once!
 * If you expect multiple  calls use \ref FutureQueue
 *
 * Future can currently only be consumed by a single thread.//TODO(ts) change that.
 *
 */
template <typename ... Ts>
class FutureX
{
public:
    //! tuple to hold all values given by callback
    using Values = std::tuple<Ts ...>;

private:
    //! Mutex for the condition variable
    std::mutex mutex_;

    //! For Notifications to the blocking thread
    std::condition_variable cv_;

    //! Indicates if emulator was triggered before waitForNext
    //! / WaitForEnd was called
    bool triggered_ = false;

    //! state that indicates whether get was already called
    bool finished_ = false;

    //! Stores the value if callback returned before next was invoked
    Values values_;

public:
    ~FutureX() {
        std::unique_lock<std::mutex> lock(mutex_);
    }

    //! This is the callback to be called to fulfill the future.
    void Callback(Ts&& ... data) {
        std::unique_lock<std::mutex> lock(mutex_);
        values_ = Values(std::forward<Ts>(data) ...);
        triggered_ = true;
        std::atomic_thread_fence(std::memory_order_release);
        cv_.notify_one();
    }

    //! Blocks until value is available and returns it
    Values && Wait() {
        assert(!finished_); //prevent multiple calls to Wait()
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return triggered_; });
        triggered_ = false;
        finished_ = true;
        return std::move(values_);
    }

    //! Returns value, must have gotten it with Wait()
    Values && Get() {
        assert(finished_);
        return std::move(values_);
    }

    //! Indicates if get was invoked and returned
    //! Can be used at the end of a job to see if outstanding
    //! futures were not called.
    bool is_finished() const noexcept {
        return finished_;
    }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_FUTURE_HEADER

/******************************************************************************/
