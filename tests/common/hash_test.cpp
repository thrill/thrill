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
#include <thrill/common/config.hpp>
#include <thrill/common/hash.hpp>
#include <thrill/common/logger.hpp>

using namespace thrill;

template <typename T>
void check_hash(uint32_t reference, const T& val) {
    common::hash_crc32_fallback<T> h;
    uint32_t crc = h(val);
    ASSERT_EQ(reference, crc);

#ifdef THRILL_HAVE_SSE4_2
    // SSE4.2 is enabled, check that one as well
    common::hash_crc32_intel<T> h2;
    uint32_t crc2 = h2(val);
    ASSERT_EQ(reference, crc2);
#endif
}

TEST(Hash, TestCRC32) {
    check_hash(0xb798b438, 0);
    check_hash(0x6add1e80, 1);
    check_hash(0xa530b397, 426468);

    char zeroes4[4], zeroes7[7], zeroes32[32], testvec[44];
    memset(&zeroes4, 0, 4);
    // 4 char-zeroes should yield the same result as a uint32_t-zero
    check_hash(0xb798b438, zeroes4);

    // Something oddly-sized
    memset(&zeroes7, 0, 7);
    check_hash(0x44c19592, zeroes7);

    // 32 bytes of zeroes - test vector at
    // https://tools.ietf.org/html/draft-ietf-tsvwg-sctpcsum-01
    memset(&zeroes32, 0, 32);
    check_hash(0x756EC955, zeroes32);

    // the other IETF test vector, 13 zeroes followed by byte values 1 to 0x1f
    memset(&testvec, 0, 13);
    for (size_t i = 1; i <= 0x1f; ++i) {
        testvec[i+12] = i;
    }
    check_hash(0x5b988D47, testvec);

    // Some more random tests
    check_hash(0x3e2fbccf, 'a'); // char
    check_hash(0x9da0355c, "a"); // char[2]
    check_hash(0x64e2a555, "hello world");
    check_hash(0x3cc762b0, "123456789");
}

/******************************************************************************/
