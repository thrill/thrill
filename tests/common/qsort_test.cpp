/*******************************************************************************
 * tests/common/qsort_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/qsort.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <vector>

using namespace thrill;

//! boxed struct being sorted: only possible to compare with explicit comparator
struct MyInteger {
    size_t i;
    explicit MyInteger(size_t _i) : i(_i) { }
};

//! comparator for MyInteger
struct MyIntegerCmp {
    bool operator () (const MyInteger& a, const MyInteger& b) const {
        return a.i < b.i;
    }
};

static void test_qsorts(const std::vector<MyInteger>& vec_input) {
    std::vector<MyInteger> vec_correct = vec_input;
    std::sort(vec_correct.begin(), vec_correct.end(), MyIntegerCmp());

    std::vector<MyInteger> vec1 = vec_input;
    common::qsort_two_pivots_yaroslavskiy(
        vec1.begin(), vec1.end(), MyIntegerCmp());

    ASSERT_EQ(vec_correct.size(), vec1.size());
    for (size_t i = 0; i < vec1.size(); ++i) {
        ASSERT_EQ(vec_correct[i].i, vec1[i].i);
    }

    std::vector<MyInteger> vec2 = vec_input;
    common::qsort_three_pivots(
        vec2.begin(), vec2.end(), MyIntegerCmp());

    ASSERT_EQ(vec_correct.size(), vec2.size());
    for (size_t i = 0; i < vec2.size(); ++i) {
        ASSERT_EQ(vec_correct[i].i, vec2[i].i);
    }
}

TEST(QSort, RandomBoxedIntegers) {

    std::default_random_engine rng(std::random_device { } ());

    // generate small vector with random boxed integers
    size_t test_size = 10240 + rng() % 20480;
    std::vector<MyInteger> vec;
    vec.reserve(test_size);

    for (size_t i = 0; i < test_size; ++i) {
        vec.emplace_back(i);
    }
    test_qsorts(vec);
}

TEST(QSort, AllEqualBoxedIntegers) {

    std::default_random_engine rng(std::random_device { } ());

    // generate small vector with all equal boxed integers
    size_t test_size = 10240 + rng() % 20480;
    std::vector<MyInteger> vec;
    vec.reserve(test_size);

    for (size_t i = 0; i < test_size; ++i) {
        vec.emplace_back(42);
    }
    test_qsorts(vec);
}

/******************************************************************************/
