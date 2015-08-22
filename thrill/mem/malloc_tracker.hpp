/*******************************************************************************
 * thrill/mem/malloc_tracker.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_MEM_MALLOC_TRACKER_HEADER
#define THRILL_MEM_MALLOC_TRACKER_HEADER

#include <cstdlib>

namespace thrill {
namespace mem {

//! bypass malloc tracker and access malloc() directly
void * bypass_malloc(size_t size) noexcept;

//! bypass malloc tracker and access free() directly
void bypass_free(void* ptr) noexcept;

//! returns the currently allocated amount of memory
size_t malloc_tracker_current();

//! returns the current peak memory allocation
size_t malloc_tracker_peak();

//! resets the peak memory allocation to current
void malloc_tracker_reset_peak();

//! returns the total number of allocations
size_t malloc_tracker_total_allocs();

//! user function which prints current and peak allocation to stderr
void malloc_tracker_print_status();

} // namespace mem
} // namespace thrill

#endif // !THRILL_MEM_MALLOC_TRACKER_HEADER

/******************************************************************************/
