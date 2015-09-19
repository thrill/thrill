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

namespace thrill {
namespace common {

//Finding cache line size is hard - we assume 64 byte. 
#define CACHE_LINE_SIZE 64 

//we can not use std::aligned_storage since it does not support
//abritary alignment. 
struct AlignedPtr {
    alignas(CACHE_LINE_SIZE) void* ptr;
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_MEMORY_HEADER

/******************************************************************************/
