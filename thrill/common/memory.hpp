/*******************************************************************************
 * thrill/common/memory.hpp
 *
 * Memory specific global defines.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_MEMORY_HEADER
#define THRILL_COMMON_MEMORY_HEADER

#include <thrill/common/config.hpp>

namespace thrill {
namespace common {

// We can not use std::aligned_storage since it does not support arbitrary
// alignment.
struct AlignedPtr {
    alignas(g_cache_line_size) void* ptr;
};

static_assert(sizeof(AlignedPtr) == g_cache_line_size,
              "AlignedPtr has incorrect size.");

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_MEMORY_HEADER

/******************************************************************************/
