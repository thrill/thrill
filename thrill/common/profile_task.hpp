/*******************************************************************************
 * thrill/common/profile_task.hpp
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
#ifndef THRILL_COMMON_PROFILE_TASK_HEADER
#define THRILL_COMMON_PROFILE_TASK_HEADER

#include <chrono>

namespace thrill {
namespace common {

// forward declarations
class ProfileThread;

class ProfileTask
{
public:
    //! virtual destructor
    virtual ~ProfileTask() { }

    //! method called by ProfileThread.
    virtual void RunTask(const std::chrono::steady_clock::time_point& tp) = 0;
};

class ProfileTaskRegistration
{
public:
    ProfileTaskRegistration(const std::chrono::milliseconds& period,
                            ProfileThread& profiler, ProfileTask* task);

    //! non-copyable: delete copy-constructor
    ProfileTaskRegistration(const ProfileTaskRegistration&) = delete;
    //! non-copyable: delete assignment operator
    ProfileTaskRegistration&
    operator = (const ProfileTaskRegistration&) = delete;

    ~ProfileTaskRegistration();

private:
    //! profiler at which the task was registered
    ProfileThread& profiler_;
    //! task to register and unregister
    ProfileTask* task_;
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_PROFILE_TASK_HEADER

/******************************************************************************/
