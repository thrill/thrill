/*******************************************************************************
 * tests/core/allocator_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/core/allocator.hpp>
#include <gtest/gtest.h>

#include <deque>
#include <vector>

using namespace c7a;

TEST(Allocator, Test1) {
    core::MemoryManager stats(nullptr);

    LOG1 << "vector";
    {
        std::vector<int, core::Allocator<int> > my_vector {
            core::Allocator<int>(&stats)
        };

        for (size_t i = 0; i < 100; ++i) {
            my_vector.push_back(i);
        }
    }
    LOG1 << "deque";
    {
        std::deque<size_t, core::Allocator<size_t> > my_deque {
            core::Allocator<size_t>(&stats)
        };

        for (size_t i = 0; i < 100; ++i) {
            my_deque.push_back(i);
        }
    }
}

namespace c7a {
namespace core {

// forced instantiations
template class Allocator<int>;

} // namespace core
} // namespace c7a

/******************************************************************************/
