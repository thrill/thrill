/*******************************************************************************
 * thrill/common/profile_thread.hpp
 *
 * A thread running a set of tasks scheduled at regular time intervals. Used in
 * Thrill for creating profiles of CPU usage, memory, etc.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_PROFILE_THREAD_HEADER
#define THRILL_COMMON_PROFILE_THREAD_HEADER

#include <thrill/common/binary_heap.hpp>
#include <thrill/common/delegate.hpp>
#include <thrill/common/profile_task.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

namespace thrill {
namespace common {

class ProfileThread
{
public:
    using milliseconds = std::chrono::milliseconds;
    using steady_clock = std::chrono::steady_clock;

    ProfileThread();

    //! non-copyable: delete copy-constructor
    ProfileThread(const ProfileThread&) = delete;
    //! non-copyable: delete assignment operator
    ProfileThread& operator = (const ProfileThread&) = delete;

    ~ProfileThread();

    //! Register a regularly scheduled callback
    template <typename Period>
    void Add(const Period& period, ProfileTask* task, bool own_task = false) {
        std::unique_lock<std::timed_mutex> lock(mutex_);
        tasks_.emplace(steady_clock::now() + period,
                       std::chrono::duration_cast<milliseconds>(period),
                       task, own_task);
        cv_.notify_one();
    }

    //! Unregister a regularly scheduled callback
    bool Remove(ProfileTask* task) {
        std::unique_lock<std::timed_mutex> lock(mutex_);
        return tasks_.erase([task](const Timer& t) { return t.task == task; });
    }

private:
    //! thread for profiling (only run on top-level loggers)
    std::thread thread_;

    //! flag to terminate profiling thread
    std::atomic<bool> terminate_ { false };

    //! cv/mutex pair to signal thread to terminate
    std::timed_mutex mutex_;

    //! cv/mutex pair to signal thread to terminate
    std::condition_variable_any cv_;

    //! struct for timer callbacks
    struct Timer
    {
        //! timepoint of next run
        steady_clock::time_point next_timeout;
        //! interval period for rescheduling
        milliseconds             period;
        //! callback
        ProfileTask              * task;
        //! delete task on deletion
        bool                     own_task;

        Timer(const steady_clock::time_point& _next_timeout,
              const milliseconds& _period,
              ProfileTask* _task, bool _own_task);

        bool operator < (const Timer& b) const;
    };

    //! priority queue of interval scheduled callbacks
    using TimerPQ = BinaryHeap<Timer>;

    //! priority queue of interval scheduled callbacks
    TimerPQ tasks_;

    //! the thread worker function
    void Worker();
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_PROFILE_THREAD_HEADER

/******************************************************************************/
