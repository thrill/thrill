/*******************************************************************************
 * tests/mem/allocator_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/mem/allocator.hpp>

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>

#include <deque>
#include <vector>

using namespace thrill;

TEST(Allocator, Vector) {
    mem::Manager mem_manager(nullptr, "TestAllocator");

    std::vector<int, mem::Allocator<int> > my_vector {
        mem::Allocator<int>(mem_manager)
    };

    for (int i = 0; i < 100; ++i) {
        my_vector.push_back(i);
    }
}

TEST(Allocator, Deque) {
    mem::Manager mem_manager(nullptr, "TestAllocator");

    std::deque<size_t, mem::Allocator<size_t> > my_deque {
        mem::Allocator<size_t>(mem_manager)
    };

    for (int i = 0; i < 100; ++i) {
        my_deque.push_back(i);
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
