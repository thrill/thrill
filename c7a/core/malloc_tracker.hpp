/*******************************************************************************
 * c7a/core/malloc_tracker.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_MALLOC_TRACKER_HEADER
#define C7A_CORE_MALLOC_TRACKER_HEADER

#include <stdlib.h>

namespace c7a {
namespace core {

//! bypass malloc tracker and access malloc() directly
void* malloc_bypass(size_t size) noexcept;

//! bypass malloc tracker and access free() directly
void free_bypass(void* ptr) noexcept;

//! returns the currently allocated amount of memory
size_t malloc_tracker_current();

//! returns the current peak memory allocation
size_t malloc_tracker_peak();

//! resets the peak memory allocation to current
void malloc_tracker_reset_peak();

//! returns the total number of allocations
size_t malloc_tracker_num_allocs();

//! user function which prints current and peak allocation to stderr
void malloc_tracker_print_status();

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_MALLOC_TRACKER_HEADER

/******************************************************************************/
