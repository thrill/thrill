/*******************************************************************************
 * thrill/common/profile_thread.cpp
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

#include <thrill/common/profile_thread.hpp>

#include <thrill/common/config.hpp>

namespace thrill {
namespace common {

/******************************************************************************/
// ProfileThread

ProfileThread::ProfileThread() {
    if (common::g_profile_thread)
        thread_ = std::thread(&ProfileThread::Worker, this);
}

ProfileThread::~ProfileThread() {
    if (common::g_profile_thread) {
        std::unique_lock<std::timed_mutex> lock(mutex_);
        terminate_ = true;
        cv_.notify_one();
        lock.unlock();
        thread_.join();
    }

    for (Timer& t : tasks_.container()) {
        if (t.own_task)
            delete t.task;
    }
}

bool ProfileThread::Remove(ProfileTask* task) {
    std::unique_lock<std::timed_mutex> lock(mutex_);
    return tasks_.erase([task](const Timer& t) { return t.task == task; }) != 0;
}

void ProfileThread::Worker() {
    std::unique_lock<std::timed_mutex> lock(mutex_);

    steady_clock::time_point tm = steady_clock::now();

    while (!terminate_)
    {
        if (tasks_.empty()) {
            cv_.wait(mutex_, [this]() { return !tasks_.empty(); });
            continue;
        }

        while (tasks_.top().next_timeout <= tm) {
            const Timer& top = tasks_.top();
            top.task->RunTask(tm);

            // requeue timeout event again.
            tasks_.emplace(top.next_timeout + top.period,
                           top.period, top.task, top.own_task);
            tasks_.pop();
        }

        tm = tasks_.top().next_timeout;
        cv_.wait_until(mutex_, tm);
        tm = steady_clock::now();
    }
}

/******************************************************************************/
// ProfileThread::Timer

ProfileThread::Timer::Timer(const steady_clock::time_point& _next_timeout,
                            const milliseconds& _period,
                            ProfileTask* _task, bool _own_task)
    : next_timeout(_next_timeout), period(_period),
      task(_task), own_task(_own_task) { }

bool ProfileThread::Timer::operator < (const Timer& b) const {
    return next_timeout > b.next_timeout;
}

/******************************************************************************/
// ProfileTaskRegistration

ProfileTaskRegistration::ProfileTaskRegistration(
    const std::chrono::milliseconds& period,
    ProfileThread& profiler, ProfileTask* task)
    : profiler_(profiler), task_(task) {
    profiler_.Add(period, task);
}

ProfileTaskRegistration::~ProfileTaskRegistration() {
    profiler_.Remove(task_);
}

} // namespace common
} // namespace thrill

/******************************************************************************/
