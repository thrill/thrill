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

    size_t token;
    int* array = reinterpret_cast<int*>(mapper.Allocate(token));

    //write into memory area to see if we get a segfault
    for(unsigned int i = 0; i * sizeof(int) < mapper.object_size() ; i++) {
        array[i] = i;
    }

    mapper.Free(token, reinterpret_cast<char*>(array));
}

TEST(PageMapper, FreeMakesMemoryAreaInaccessible) {
    mem::PageMapper mapper;

    size_t token;
    int* array = reinterpret_cast<int*>(mapper.Allocate(token));
    mapper.Free(token, reinterpret_cast<char*>(array));

    ASSERT_DEATH({ array[1] = 42; }, "");
}

TEST(PageMapper, SwapOutLeavesAreaInaccessible) {
    mem::PageMapper mapper;

    size_t token;
    int* array = reinterpret_cast<int*>(mapper.Allocate(token));
    mapper.SwapOut(reinterpret_cast<char*>(array));

    ASSERT_DEATH({ array[1] = 42; }, "");
    mapper.Free(token);
}

TEST(PageMapper, SwapInLeavesMakesAreaAccessible) {
    mem::PageMapper mapper;

    size_t token;
    int* array = reinterpret_cast<int*>(mapper.Allocate(token));
    mapper.SwapOut(reinterpret_cast<char*>(array));
    array = reinterpret_cast<int*>(mapper.SwapIn(token));

    //write into memory area to see if we get a segfault
    for(unsigned int i = 0; i * sizeof(int) < mapper.object_size() ; i++) {
        array[i] = i;
    }

    mapper.Free(token, reinterpret_cast<char*>(array));
}

TEST(PageMapper, SwappingMultiplePagesDoesNotAlterContent) {
    mem::PageMapper mapper;

    //write ascending numbers into array 1 & swap out
    size_t token1;
    int* array1 = reinterpret_cast<int*>(mapper.Allocate(token1));
    for(unsigned int i = 0; i * sizeof(int) < mapper.object_size() ; i++) {
        array1[i] = i;
    }
    mapper.SwapOut(reinterpret_cast<char*>(array1));

    //write descending numbers into array 2 & swap out
    size_t token2;
    int* array2 = reinterpret_cast<int*>(mapper.Allocate(token2));
    for(unsigned int i = 0; i * sizeof(int) < mapper.object_size() ; i++) {
        array2[i] = mapper.object_size() - i;
    }
    mapper.SwapOut(reinterpret_cast<char*>(array2));

    //read array1
    array1 = reinterpret_cast<int*>(mapper.SwapIn(token1));
    for(unsigned int i = 0; i * sizeof(int) < mapper.object_size() ; i++) {
        ASSERT_EQ(i, array1[i]);
    }

    //read array2
    array2 = reinterpret_cast<int*>(mapper.SwapIn(token2));
    for(unsigned int i = 0; i * sizeof(int) < mapper.object_size() ; i++) {
        ASSERT_EQ(mapper.object_size() - i, array2[i]);
    }

    mapper.Free(token1, reinterpret_cast<char*>(array1));
    mapper.Free(token2, reinterpret_cast<char*>(array2));
}

namespace thrill {
namespace mem {

// forced instantiations
template class FixedAllocator<int, g_bypass_manager>;
template class Allocator<int>;

} // namespace mem
} // namespace thrill

/******************************************************************************/
