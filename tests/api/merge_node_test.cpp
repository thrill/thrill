/*******************************************************************************
 * tests/api/merge_node_test.cpp
 *
 * Part of Project thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gemail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/api/allgather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/merge.hpp>
#include <thrill/common/string.hpp>
#include <gtest/gtest.h>
#include <thrill/data/block_queue.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <vector>

using namespace thrill;

using thrill::api::Context;
using thrill::api::DIA;

template <typename stackA, typename stackB>
void DoMergeAndCheckResult(api::DIA<size_t, stackA> merge_input1, api::DIA<size_t, stackB> merge_input2, size_t expected_size, int num_workers) {
        // merge
        auto merge_result = merge_input1.Merge(
            merge_input2, std::less<size_t>());

        // check if order was kept while merging. 
        int count = 0;
        auto res = merge_result.Map([&count] (size_t in) { 
                count++; 
                return in; 
        }).AllGather();

        ASSERT_EQ(expected_size, res.size());

        for (size_t i = 0; i != res.size() - 1; ++i) {
            ASSERT_TRUE(res[i] <= res[i + 1]);
        }

        // REVIEW(ej): check CONTENTS of res as well!
        
        //static const bool debug = true;

        //LOG << "count: " << count << " expected: " << ((float)res.size() / (float)num_workers);

        // check if balancing condition was met
        // TODO(EJ) There seems to be a bug with inbalanced arrays on a very low number
        // Of workers. I'm not sure why though. 
        ASSERT_TRUE(std::abs((float)res.size() / (float)num_workers - count) <= num_workers + 50);
}

TEST(MergeNode, TwoBalancedIntegerArrays) {

    const size_t test_size = 5000;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // even numbers in 0..9998 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx,
                [](size_t index) { return index * 2; },
                test_size);

            // odd numbers in 1..9999
            auto merge_input2 = merge_input1.Map(
                [](size_t i) { return i + 1; } );

            DoMergeAndCheckResult(merge_input1, merge_input2, test_size * 2, ctx.num_workers());
        };

    thrill::api::RunLocalTests(start_func);
}

TEST(MergeNode, TwoImbalancedIntegerArrays) {

    const size_t test_size = 5000;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // numbers in 0..9998 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx,
                [](size_t index) { return index ; },
                test_size);

            // numbers in 10000..19998
            auto merge_input2 = merge_input1.Map(
                [](size_t i) { return i + 10000; } );

            DoMergeAndCheckResult(merge_input1, merge_input2, test_size * 2, ctx.num_workers());
        };

    thrill::api::RunLocalTests(start_func);
}

TEST(MergeNode, TwoIntegerArraysOfDifferentSize) {

    const size_t test_size = 5000;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // numbers in 0..50 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx,
                [](size_t index) { return index ; },
                test_size);

            // numbers in 10..110
            auto merge_input2 = Generate(
                ctx,
                [](size_t index) { return index + 10; },
                test_size * 2);

            DoMergeAndCheckResult(merge_input1, merge_input2, test_size * 3, ctx.num_workers());
        };

    thrill::api::RunLocalTests(start_func);
}

// REVIEW(ej): test another data type, one which is not default constructible!
