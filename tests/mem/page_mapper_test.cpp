/*******************************************************************************
 * tests/mem/page_mapper_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>
#include <thrill/mem/page_mapper.hpp>

using namespace thrill;

TEST(PageMapper, AllocateReturnsAccessibleMemoryArea) {
    mem::PageMapper mapper;

    int* array = reinterpret_cast<int*>(mapper.Allocate());

    //write into memory area to see if we get a segfault
    for(unsigned int i = 0; i * sizeof(int) < mapper.object_size() ; i++) {
        array[i] = i;
    }

    mapper.Free(reinterpret_cast<char*>(array));
}

TEST(PageMapper, FreeMakesMemoryAreaInaccessible) {
    mem::PageMapper mapper;

    int* array = reinterpret_cast<int*>(mapper.Allocate());
    mapper.Free(reinterpret_cast<char*>(array));

    ASSERT_DEATH({ array[1] = 42; }, "Segfault");
}

TEST(PageMapper, SwapOutLeavesAreaAccessible) {
    mem::PageMapper mapper;

    int* array = reinterpret_cast<int*>(mapper.Allocate());
    mapper.SwapOut(reinterpret_cast<char*>(array));

    //write into memory area to see if we get a segfault
    for(unsigned int i = 0; i < mapper.object_size(); i += sizeof(int)) {
        array[i] = i;
    }

    mapper.Free(reinterpret_cast<char*>(array));
}

TEST(PageMapper, PrefetchLeavesAreaAccessible) {
    mem::PageMapper mapper;

    int* array = reinterpret_cast<int*>(mapper.Allocate());
    mapper.Prefetch(reinterpret_cast<char*>(array));

    //write into memory area to see if we get a segfault
    for(unsigned int i = 0; i < mapper.object_size(); i += sizeof(int)) {
        array[i] = i;
    }

    mapper.Free(reinterpret_cast<char*>(array));
}

namespace thrill {
namespace mem {

// forced instantiations
template class FixedAllocator<int, g_bypass_manager>;
template class Allocator<int>;

} // namespace mem
} // namespace thrill

/******************************************************************************/
