/*******************************************************************************
 * thrill/common/signal.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_SIGNAL_HEADER
#define THRILL_COMMON_SIGNAL_HEADER

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace thrill {
namespace common {

/*!
 * Signal helps you deal with the nasty callbacks in the land of callback-hell.
 *
 * Problems with condition variable the following:
 *
 * | consumer     |   producer
 * | |--req(&cv)--|---->req(cv)
 * |              |      |-do_stuff()
 * |              |      |-cv.notify_one()
 * | |cv.wait()   |
 * which can cause deadlocks.
 * A Signal is a one-time trigger. It can perform a state change exactly once
 * After that all consecuitve calls to 'Wait' return immediately.
 * Multiple threads can wait concurrently on a signal and will all be woke up
 * together.
 *
 * common::Future<T> offers the same functionality with the addition
 * of moving a data element between the threads.
 */
class Signal
{
protected:
    //! Mutex for the condition variable
    std::mutex mutex_;

    //! For Notifications to the blocking thread
    std::condition_variable cv_;

    //! Indicates if emulator was set before waitForNext
    //! / WaitForEnd was called
    std::atomic<bool> set_ { false };

public:
    ~Signal() {
        std::unique_lock<std::mutex> lock(mutex_);
    }

    //! Blocks until signal was set
    //! returns immediately if signal was already set
    void Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (set_) return;
        cv_.wait(lock);
    }

    void Set() {
        set_ = true;
        cv_.notify_all();
    }
};


} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_SIGNAL_HEADER

/******************************************************************************/
