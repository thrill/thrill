/*******************************************************************************
 * tests/mem/page_mapper_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>
#include <thrill/mem/page_mapper.hpp>

using namespace thrill;

TEST(PageMapper, AllocateReturnsAccessibleMemoryArea) {
    mem::PageMapper<4096> mapper;

    size_t token;
    int* array = reinterpret_cast<int*>(mapper.Allocate(token));

    //write into memory area to see if we get a segfault
    for(unsigned int i = 0; i * sizeof(int) < 4096 ; i++) {
        array[i] = i;
    }

    mapper.ReleaseToken(token);
}

TEST(PageMapper, SwapOutLeavesAreaInaccessible) {
    mem::PageMapper<4096> mapper;

    size_t token;
    int* array = reinterpret_cast<int*>(mapper.Allocate(token));
    mapper.SwapOut(reinterpret_cast<char*>(array));

    ASSERT_DEATH({ array[1] = 42; }, "");
    mapper.ReleaseToken(token);
}

TEST(PageMapper, SwapInLeavesMakesAreaAccessible) {
    mem::PageMapper<4096> mapper;

    size_t token;
    int* array = reinterpret_cast<int*>(mapper.Allocate(token));
    mapper.SwapOut(reinterpret_cast<char*>(array));
    array = reinterpret_cast<int*>(mapper.SwapIn(token));

    //write into memory area to see if we get a segfault
    for(unsigned int i = 0; i * sizeof(int) < 4096 ; i++) {
        array[i] = i;
    }

    mapper.ReleaseToken(token);
}

TEST(PageMapper, SwappingMultiplePagesDoesNotAlterContent) {
    mem::PageMapper<4096> mapper;

    //write ascending numbers into array 1 & swap out
    size_t token1;
    int* array1 = reinterpret_cast<int*>(mapper.Allocate(token1));
    for(unsigned int i = 0; i * sizeof(int) < 4096 ; i++) {
        array1[i] = i;
    }
    mapper.SwapOut(reinterpret_cast<char*>(array1));

    //write descending numbers into array 2 & swap out
    size_t token2;
    int* array2 = reinterpret_cast<int*>(mapper.Allocate(token2));
    for(unsigned int i = 0; i * sizeof(int) < 4096 ; i++) {
        array2[i] = 4096 - i;
    }
    mapper.SwapOut(reinterpret_cast<char*>(array2));

    //read array1
    array1 = reinterpret_cast<int*>(mapper.SwapIn(token1));
    for(unsigned int i = 0; i * sizeof(int) < 4096 ; i++) {
        ASSERT_EQ(i, array1[i]);
    }

    //read array2
    array2 = reinterpret_cast<int*>(mapper.SwapIn(token2));
    for(unsigned int i = 0; i * sizeof(int) < 4096 ; i++) {
        ASSERT_EQ(4096 - i, array2[i]);
    }

    mapper.ReleaseToken(token1);
    mapper.ReleaseToken(token2);
}

namespace thrill {
namespace mem {

// forced instantiations
template class FixedAllocator<int, g_bypass_manager>;
template class Allocator<int>;

} // namespace mem
} // namespace thrill

/******************************************************************************/
