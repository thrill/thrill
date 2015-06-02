/*******************************************************************************
 * c7a/common/future.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_FUTURE_HEADER
#define C7A_COMMON_FUTURE_HEADER

#include <condition_variable>
#include <mutex>
#include <cassert>

namespace c7a {
namespace common {

/*!
 * Future helps you deal with the nasty callbacks in the land of callback-hell.
 *
 * Handles the use-case where a callback is expected to be called exactly once!
 *
 * Waiter can currently only be consumed by a single thread.//TODO(ts) change that.
 */
template <typename T>
class Future
{
protected:
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
    T last_;

public:
    typedef std::function<void (T&&, bool)> CallbackFn;

    CallbackFn GetCallback() {
        return [=](T && data, bool finished) {
                   assert(finished);
                   last_ = data;
                   triggered_ = true;
                   cv_.notify_one();
        };
    }

    //! Blocks until value is available and returns it
    T && Get() {
        assert(!finished_); //prevent multiple calls to Get()
        if (!triggered_) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock);
        }
        triggered_ = false;
        finished_ = true;
        return std::move(last_);
    }

    //! Indicates if get was invoked and returned
    //! Can be used at the end of a job to see if outstanding
    //! futures were not called.
    bool is_finished() {
        return finished_;
    }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_FUTURE_HEADER

/******************************************************************************/
