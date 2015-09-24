/*******************************************************************************
 * tests/common/static_ring_buffer_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>
#include <thrill/common/static_ring_buffer.hpp>

using namespace thrill;

TEST(StaticRingBuffer, FillCircular) {

    common::StaticRingBuffer<size_t> ring(12);

    // put first element
    ASSERT_EQ(0u, ring.size());
    ring.push_back(0);

    // put nine more
    for (size_t i = 1; i < 10; ++i) {
        ASSERT_EQ(i, ring.size());
        ring.emplace_back(i);
    }

    ASSERT_EQ(10u, ring.size());

    // check contents of ring buffer
    for (size_t i = 0; i < ring.size(); ++i) {
        ASSERT_EQ(i, ring[i]);
    }

    for (size_t j = 0; j < 1000; ++j) {
        // check contents of ring buffer
        for (size_t i = 0; i < ring.size(); ++i) {
            ASSERT_EQ(j + i, ring[i]);
        }
        ASSERT_EQ(j, ring.front());
        ASSERT_EQ(j + 9u, ring.back());

        // append an item, and remove one
        ring.push_back(j + 10u);
        ring.pop_front();
        ASSERT_EQ(10u, ring.size());
    }
}

struct MyStruct {
    int i1, i2;

    MyStruct(int _i1, int _i2)
        : i1(_i1), i2(_i2) { }
};

TEST(StaticRingBuffer, NonDefaultConstructible) {

    common::StaticRingBuffer<MyStruct> ring(12);

    ring.push_back(MyStruct(0, 1));
    ring.emplace_back(1, 2);

    ring.push_front(MyStruct(2, 3));
    ring.emplace_front(3, 4);

    ASSERT_EQ(4u, ring.size());
    ASSERT_EQ(3u, ring[0].i1);
    ASSERT_EQ(2u, ring[1].i1);
    ASSERT_EQ(0u, ring[2].i1);
    ASSERT_EQ(1u, ring[3].i1);
}

namespace thrill {
namespace common {

template class StaticRingBuffer<size_t>;

} // namespace common
} // namespace thrill

/******************************************************************************/
