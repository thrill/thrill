/*******************************************************************************
 * tests/api/merge_node_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/all_gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/merge.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/string.hpp>
#include <thrill/data/block_queue.hpp>

#include <algorithm>
#include <functional>
#include <random>
#include <string>
#include <vector>

using namespace thrill;

template <typename InputA, typename... MoreInputs>
void DoMergeAndCheckResult(
    const std::vector<size_t>& expected,
    const InputA& merge_input1, const MoreInputs& ... merge_inputs) {

    size_t num_workers = merge_input1.context().num_workers();

    // merge
    auto merge_result = Merge(
        std::less<size_t>(), merge_input1, merge_inputs...);

    // crude method to calculate the number of local items
    size_t count = 0;
    auto res = merge_result
               .Map([&count](size_t in) { count++; return in; })
               .AllGather();

    // Check if res is as expected.
    ASSERT_EQ(expected, res);

    // check if balancing condition was met
    LOG0 << "count: " << count << " expected: " << res.size() / num_workers;
    size_t expectedCount = res.size() / num_workers;
    ASSERT_LE(tlx::abs_diff(expectedCount, count), num_workers + 50);
}

TEST(MergeNode, TwoBalancedIntegerArrays) {

    static constexpr size_t test_size = 5000;

    auto start_func =
        [](Context& ctx) {

            // even numbers in 0..9998 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx, test_size,
                [](size_t index) { return index * 2; });

            // odd numbers in 1..9999
            auto merge_input2 = merge_input1.Map(
                [](size_t i) { return i + 1; });

            std::vector<size_t> expected(test_size * 2);
            for (size_t i = 0; i < test_size * 2; i++) {
                expected[i] = i;
            }

            DoMergeAndCheckResult(expected, merge_input1, merge_input2);
        };

    api::RunLocalTests(start_func);
}

TEST(MergeNode, FourBalancedIntegerArrays) {

    static constexpr size_t test_size = 5000;

    auto start_func =
        [](Context& ctx) {

            auto merge_input1 = Generate(
                ctx, test_size,
                [](size_t index) { return index * 4; });

            auto merge_input2 = merge_input1.Map(
                [](size_t i) { return i + 1; });
            auto merge_input3 = merge_input1.Map(
                [](size_t i) { return i + 2; });
            auto merge_input4 = merge_input1.Map(
                [](size_t i) { return i + 3; });

            std::vector<size_t> expected(test_size * 4);
            for (size_t i = 0; i < test_size * 4; i++) {
                expected[i] = i;
            }

            DoMergeAndCheckResult(
                expected,
                merge_input1, merge_input2, merge_input3, merge_input4);
        };

    api::RunLocalTests(start_func);
}

TEST(MergeNode, TwoImbalancedIntegerArrays) {

    static constexpr size_t test_size = 5000;

    auto start_func =
        [](Context& ctx) {

            // numbers in 0..4999 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx, test_size,
                [](size_t index) { return index; });

            // numbers in 10000..14999
            auto merge_input2 = merge_input1.Map(
                [](size_t i) { return i + 10000; });

            std::vector<size_t> expected;
            expected.reserve(test_size * 2);

            for (size_t i = 0; i < test_size; i++) {
                expected.push_back(i);
            }

            for (size_t i = 0; i < test_size; i++) {
                expected.push_back(i + 10000);
            }

            DoMergeAndCheckResult(expected, merge_input1, merge_input2);
        };

    api::RunLocalTests(start_func);
}

TEST(MergeNode, TwoIntegerArraysOfDifferentSize) {

    static constexpr size_t test_size = 5000;
    static constexpr size_t offset = 2500;

    auto start_func =
        [](Context& ctx) {

            // numbers in 0..4999 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx, test_size,
                [](size_t index) { return index; });

            // numbers in 2500..12499
            auto merge_input2 = Generate(
                ctx, test_size * 2,
                [](size_t index) { return index + offset; });

            std::vector<size_t> expected;
            expected.reserve(test_size * 3);

            for (size_t i = 0; i < test_size; i++) {
                expected.push_back(i);
            }

            for (size_t i = 0; i < test_size * 2; i++) {
                expected.push_back(i + offset);
            }

            std::sort(expected.begin(), expected.end());

            DoMergeAndCheckResult(expected, merge_input1, merge_input2);
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
