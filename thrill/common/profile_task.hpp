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

class ProfileTask
{
public:
    //! virtual destructor
    virtual ~ProfileTask() { }

    //! method called by ProfileThread.
    virtual void RunTask(const std::chrono::steady_clock::time_point& tp) = 0;
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_PROFILE_TASK_HEADER

/******************************************************************************/
