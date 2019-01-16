/*******************************************************************************
 * tests/common/radix_sort_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/radix_sort.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <vector>

using namespace thrill;

struct MyString {
    uint8_t chars[16];

    bool operator < (const MyString& b) const {
        return std::lexicographical_compare(
            chars, chars + 16, b.chars, b.chars + 16);
    }

    // method used by radix sort to access 8-bit key at given depth
    const uint8_t& at_radix(size_t depth) const { return chars[depth]; }
};

TEST(RadixSort, RandomStrings) {

    std::default_random_engine rng(std::random_device { } ());

    // generate small vector with random boxed strings
    size_t test_size = 1024000 + rng() % 20480;
    std::vector<MyString> vec;
    vec.reserve(test_size);

    for (size_t i = 0; i < test_size; ++i) {
        vec.emplace_back(MyString());
        for (size_t j = 0; j < 16; ++j) {
            vec.back().chars[j] = static_cast<uint8_t>(rng() % 10);
        }
    }

    common::radix_sort_CI<16>(vec.begin(), vec.end(), 256);

    ASSERT_TRUE(std::is_sorted(vec.begin(), vec.end()));
}

/******************************************************************************/
