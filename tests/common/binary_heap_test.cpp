/*******************************************************************************
 * tests/common/binary_heap_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/binary_heap.hpp>

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>

#include <functional>

using namespace thrill;

TEST(BinaryHeap, Fill) {

    common::BinaryHeap<size_t, std::greater<size_t> > heap;

    // put first element
    ASSERT_EQ(0u, heap.size());
    heap.emplace(0);

    // put ten more
    for (size_t i = 10; i > 0; --i) {
        heap.emplace(i);
    }

    ASSERT_EQ(11u, heap.size());

    // extract two elements
    ASSERT_EQ(0u, heap.top());
    heap.pop();
    ASSERT_EQ(1u, heap.top());
    heap.pop();

    ASSERT_EQ(9u, heap.size());

    // erase three and eight
    heap.erase([](const size_t& i) { return i == 3 || i == 8; });
    ASSERT_EQ(7u, heap.size());

    // extract remaining elements
    ASSERT_EQ(2u, heap.top());
    heap.pop();
    ASSERT_EQ(4u, heap.top());
    heap.pop();
    ASSERT_EQ(5u, heap.top());
    heap.pop();
    ASSERT_EQ(6u, heap.top());
    heap.pop();
    ASSERT_EQ(7u, heap.top());
    heap.pop();
    ASSERT_EQ(9u, heap.top());
    heap.pop();
    ASSERT_EQ(10u, heap.top());
    heap.pop();

    ASSERT_TRUE(heap.empty());
}

namespace thrill {
namespace common {

template class BinaryHeap<size_t>;

} // namespace common
} // namespace thrill

/******************************************************************************/
