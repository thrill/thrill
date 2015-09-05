/*******************************************************************************
 * tests/mem/allocator_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>
#include <thrill/mem/allocator.hpp>

#include <deque>
#include <vector>

using namespace thrill;

TEST(Allocator, Test1) {
    mem::Manager mem_manager(nullptr, "TestAllocator");

    LOG1 << "vector";
    {
        std::vector<int, mem::Allocator<int> > my_vector {
            mem::Allocator<int>(mem_manager)
        };

        for (size_t i = 0; i < 100; ++i) {
            my_vector.push_back(i);
        }
    }
    LOG1 << "deque";
    {
        std::deque<size_t, mem::Allocator<size_t> > my_deque {
            mem::Allocator<size_t>(mem_manager)
        };

        for (size_t i = 0; i < 100; ++i) {
            my_deque.push_back(i);
        }
    }
}

namespace thrill {
namespace mem {

// forced instantiations
template class FixedAllocator<int, g_bypass_manager>;
template class Allocator<int>;

} // namespace mem
} // namespace thrill

/******************************************************************************/
