/*******************************************************************************
 * tests/common/hash_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/hash.hpp>
#include <thrill/common/logger.hpp>

using namespace thrill;

template <typename T>
uint32_t hash(const T& val) {
    common::hash_crc32<T> h;
    return h(val);
}

TEST(Hash, TestCRC32) {
    ASSERT_EQ(0xb798b438, hash<uint32_t>(0));
    ASSERT_EQ(0x6add1e80, hash<uint32_t>(1));
    ASSERT_EQ(0xa530b397, hash<uint32_t>(426468));

    char zeroes4[4], zeroes7[7], zeroes32[32], testvec[44];
    memset(&zeroes4, 0, 4);
    // 4 char-zeroes should yield the same result as a uint32_t-zero
    ASSERT_EQ(0xb798b438, hash(zeroes4));

    // Something oddly-sized
    memset(&zeroes7, 0, 7);
    ASSERT_EQ(0x44c19592, hash(zeroes7));

    // 32 bytes of zeroes - test vector at
    // https://tools.ietf.org/html/draft-ietf-tsvwg-sctpcsum-01
    memset(&zeroes32, 0, 32);
    ASSERT_EQ(0x756EC955, hash(zeroes32));

    // the other IETF test vector, 13 zeroes followed by byte values 1 to 0x1f
    memset(&testvec, 0, 13);
    for (size_t i = 1; i <= 0x1f; ++i) {
        testvec[i+12] = i;
    }
    ASSERT_EQ(0x5b988D47, hash(testvec));

    // Some more random tests
    ASSERT_EQ(0x3e2fbccf, hash('a')); // char
    ASSERT_EQ(0x9da0355c, hash("a")); // char[2]
    ASSERT_EQ(0x64e2a555, hash("hello world"));
    ASSERT_EQ(0x3cc762b0, hash("123456789"));
}

/******************************************************************************/
