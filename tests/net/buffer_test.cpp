/*******************************************************************************
 * tests/net/buffer_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/net/buffer.hpp>

#include <algorithm>

using c7a::net::Buffer;

TEST(Buffer, Test1) {
    Buffer b1;
    Buffer b2(42);

    // iterator-based filling
    for (unsigned char& c : b2)
        c = 6 * 9;

    // move over
    b1 = std::move(b2);

    // check that it actually moved
    ASSERT_EQ(b1.size(), 42u);
    ASSERT_EQ(b1[0], 6 * 9);

    // ADL swap
    swap(b1, b2);

    {
        // swap using two moves.
        using std::swap;
        std::swap(b1, b2);
        std::swap(b1, b2);
    }

    ASSERT_EQ(b1.size(), 0u);
    ASSERT_EQ(b2.size(), 42u);

    b1.Resize(60);
    ASSERT_EQ(b1.size(), 60u);

    // over-move of b1
    b1 = std::move(b2);
}

/******************************************************************************/
