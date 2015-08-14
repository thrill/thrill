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

using namespace c7a::common;

TEST(NewAllocator, Test1) {
    LOG1 << "vector";
    {
        std::vector<int, NewAllocator<int> > my_vector;

        for (size_t i = 0; i < 100; ++i) {
            my_vector.push_back(i);
        }
    }
    LOG1 << "deque";
    {
        std::deque<int, NewAllocator<int> > my_deque;

        for (size_t i = 0; i < 100; ++i) {
            my_deque.push_back(i);
        }
    }
}

// forced instantiations
template class NewAllocator<int>;

/******************************************************************************/
