/*******************************************************************************
 * thrill/common/linux_proc_stats.hpp
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
#ifndef THRILL_COMMON_LINUX_PROC_STATS_HEADER
#define THRILL_COMMON_LINUX_PROC_STATS_HEADER

namespace thrill {
namespace common {

// forward declarations
class JsonLogger;
class ScheduleThread;

//! launch profiler task
void StartLinuxProcStatsProfiler(ScheduleThread& sched, JsonLogger& logger);

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_LINUX_PROC_STATS_HEADER

/******************************************************************************/
