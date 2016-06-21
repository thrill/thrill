/*******************************************************************************
 * tests/common/uint_types_test.cpp
 *
 * Class representing a 40-bit or 48-bit unsigned integer encoded in five or
 * six bytes.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/uint_types.hpp>

#include <gtest/gtest.h>

#include <limits>

using namespace thrill;

// forced instantiation
template class thrill::common::UIntPair<uint8_t>;
template class thrill::common::UIntPair<uint16_t>;

template <typename uint>
void dotest(unsigned int nbytes) {
    // simple initialize
    uint a = 42;

    // check sizeof (again)
    ASSERT_EQ(sizeof(a), nbytes);

    // count up 1024 and down again
    uint b = 0xFFFFFF00;
    uint b_save = b;

    uint64_t b64 = b;
    for (uint32_t i = 0; i < 1024; ++i)
    {
        ASSERT_EQ(b.u64(), b64);
        ASSERT_EQ(b.ull(), b64);
        ASSERT_NE(b, a);
        ++b, ++b64;
    }

    ASSERT_NE(b, b_save);

    for (uint32_t i = 0; i < 1024; ++i)
    {
        ASSERT_EQ(b.u64(), b64);
        ASSERT_EQ(b.ull(), b64);
        ASSERT_NE(b, a);
        --b, --b64;
    }

    ASSERT_EQ(b.u64(), b64);
    ASSERT_EQ(b.ull(), b64);
    ASSERT_EQ(b, b_save);

    // check min and max value
    ASSERT_LE(uint::min(), a);
    ASSERT_GE(uint::max(), a);

    ASSERT_LT(std::numeric_limits<uint>::min(), a);
    ASSERT_GT(std::numeric_limits<uint>::max(), a);

    // check simple math
    a = 42;
    a = a + a;
    ASSERT_EQ(a, uint(84));
    ASSERT_EQ(a.ull(), 84);

    a += uint(0xFFFFFF00);
    ASSERT_EQ(a.ull(), 0xFFFFFF54llu);

    a += uint(0xFFFFFF00);
    ASSERT_EQ(a.ull(), 0x1FFFFFE54llu);

    a -= uint(0xFFFFFF00);
    ASSERT_EQ(a.ull(), 0xFFFFFF54llu);

    a -= uint(0xFFFFFF00);
    ASSERT_EQ(a.ull(), 84);
}

TEST(UIntPair, Uint40) {
    dotest<common::uint40>(5);
}

TEST(UIntPair, Uint48) {
    dotest<common::uint48>(6);
}

/******************************************************************************/
