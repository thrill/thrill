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

#include <algorithm>
#include <random>
#include <string>
#include <vector>

using namespace thrill;

using thrill::api::Context;
using thrill::api::DIARef;

struct MyStruct {
    int a, b;
};

static const size_t test_size = 500;

TEST(MergeNode, TwoBalancedIntegerArrays) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // even numbers in 0..998 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx,
                [](size_t index) { return index * 2; },
                test_size);

            // odd numbers in 1..9999
            auto merge_input2 = merge_input1.Map(
                [](size_t i) { return i + 1; } );

            // merge
            auto merge_result = merge_input1.Merge(
                merge_input2, [](size_t a, size_t b) -> bool { return a < b; });

            // check if order was kept while merging. 
            auto res = merge_result.AllGather();

            ASSERT_EQ(test_size * 2, res.size());

            for (size_t i = 0; i != res.size() - 1; ++i) {
                ASSERT_TRUE(res[i] < res[i + 1]);
            }
        };

    thrill::api::RunLocalTests(start_func);
}
