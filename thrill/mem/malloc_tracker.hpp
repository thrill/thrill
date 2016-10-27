/*******************************************************************************
 * thrill/mem/malloc_tracker.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_MEM_MALLOC_TRACKER_HEADER
#define THRILL_MEM_MALLOC_TRACKER_HEADER

#include <cstdlib>

#if defined(_MSC_VER)
// windows/msvc is a mess.
#include <BaseTsd.h>
using ssize_t = SSIZE_T;
#endif

namespace thrill {
namespace common {

// forward declarations
class ProfileThread;
class JsonLogger;

} // namespace common
namespace mem {

//! boolean indication that the memory limit is exceeded
extern bool memory_exceeded;

//! method to flush thread-local memory statistics when memory_exceeded
void flush_memory_statistics();

//! set the malloc tracking system to set memory_exceeded when this limit is
//! exceed. it does not actually limit allocation!
void set_memory_limit_indication(ssize_t size);

//! bypass malloc tracker and access malloc() directly
void * bypass_malloc(size_t size) noexcept;

//! bypass malloc tracker and access free() directly
void bypass_free(void* ptr, size_t size) noexcept;

//! returns the currently allocated amount of memory
ssize_t malloc_tracker_current();

//! returns the current peak memory allocation
ssize_t malloc_tracker_peak();

//! resets the peak memory allocation to current
void malloc_tracker_reset_peak();

//! returns the total number of allocations
ssize_t malloc_tracker_total_allocs();

//! user function which prints current and peak allocation to stderr
void malloc_tracker_print_status();

//! launch profiler task
void StartMemProfiler(common::ProfileThread& sched, common::JsonLogger& logger);

} // namespace mem
} // namespace thrill

#endif // !THRILL_MEM_MALLOC_TRACKER_HEADER

/******************************************************************************/
