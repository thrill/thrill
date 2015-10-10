/*******************************************************************************
 * tests/api/merge_node_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/allgather.hpp>
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

using thrill::api::Context;
using thrill::api::DIA;

template <typename stackA, typename stackB>
void DoMergeAndCheckResult(api::DIA<size_t, stackA> merge_input1, api::DIA<size_t, stackB> merge_input2, const std::vector<size_t>& expected, int num_workers) {
    // merge
    auto merge_result = merge_input1.Merge(
            merge_input2, std::less<size_t>());

    // check if order was kept while merging.
    int count = 0;
    auto res = merge_result.Map([&count](size_t in) {
                                    count++;
                                    return in;
                                }).AllGather();

    // Check if res is as expected.
    ASSERT_EQ(expected, res);

    // check if balancing condition was met
    // TODO(EJ) There seems to be a bug with inbalanced arrays on a very
    // low number of workers. I'm not sure why though.
    // LOG << "count: " << count << " expected: " << ((float)res.size() / (float)num_workers);
    float expectedCount = (float)res.size() / (float)num_workers;
    ASSERT_TRUE(std::abs(expectedCount - count) <= num_workers + 50);
}

TEST(MergeNode, TwoBalancedIntegerArrays) {

    const size_t test_size = 5000;

    std::function<void(Context&)> start_func =
        [test_size](Context& ctx) {

            // even numbers in 0..9998 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx,
                [](size_t index) { return index * 2; },
                test_size);

            // odd numbers in 1..9999
            auto merge_input2 = merge_input1.Map(
                [](size_t i) { return i + 1; });

            std::vector<size_t> expected(test_size * 2);
            for (size_t i = 0; i < test_size * 2; i++) {
                expected[i] = i;
            }

            DoMergeAndCheckResult(merge_input1, merge_input2, expected, ctx.num_workers());
        };

    thrill::api::RunLocalTests(start_func);
}

TEST(MergeNode, TwoImbalancedIntegerArrays) {

    const size_t test_size = 5000;

    std::function<void(Context&)> start_func =
        [test_size](Context& ctx) {

            // numbers in 0..4999 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx,
                [](size_t index) { return index; },
                test_size);

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

            DoMergeAndCheckResult(merge_input1, merge_input2, expected, ctx.num_workers());
        };

    thrill::api::RunLocalTests(start_func);
}

TEST(MergeNode, TwoIntegerArraysOfDifferentSize) {

    const size_t test_size = 5000;
    const size_t offset = 2500;

    std::function<void(Context&)> start_func =
        [test_size, offset](Context& ctx) {

            // numbers in 0..4999 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx,
                [](size_t index) { return index; },
                test_size);

            // numbers in 2500..12499
            auto merge_input2 = Generate(
                ctx,
                [=](size_t index) { return index + offset; },
                test_size * 2);

            std::vector<size_t> expected;
            expected.reserve(test_size * 3);

            for (size_t i = 0; i < test_size; i++) {
                expected.push_back(i);
            }

            for (size_t i = 0; i < test_size * 2; i++) {
                expected.push_back(i + offset);
            }

            std::sort(expected.begin(), expected.end());

            DoMergeAndCheckResult(merge_input1, merge_input2, expected, ctx.num_workers());
        };

    thrill::api::RunLocalTests(start_func);
}

// REVIEW(ej): test another data type, one which is not default constructible!

/******************************************************************************/
