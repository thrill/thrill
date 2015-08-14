/*******************************************************************************
 * tests/common/allocator_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/common/allocator.hpp>
#include <gtest/gtest.h>

#include <deque>
#include <vector>

using namespace c7a;

TEST(NewAllocator, Test1) {
    common::MemoryManager stats;

    LOG1 << "vector";
    {
        std::vector<int, common::NewAllocator<int> > my_vector {
            common::NewAllocator<int>(&stats)
        };

        for (size_t i = 0; i < 100; ++i) {
            my_vector.push_back(i);
        }
    }
    LOG1 << "deque";
    {
        std::deque<size_t, common::NewAllocator<size_t> > my_deque {
            common::NewAllocator<size_t>(&stats)
        };

        for (size_t i = 0; i < 100; ++i) {
            my_deque.push_back(i);
        }
    }
}

namespace c7a {
namespace common {

// forced instantiations
template class NewAllocator<int>;

} // namespace common
} // namespace c7a

/******************************************************************************/
