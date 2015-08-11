/*******************************************************************************
 * tests/api/sort_node_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/allgather.hpp>
#include <c7a/api/generate.hpp>
#include <c7a/api/sort.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using c7a::api::Context;
using c7a::api::DIARef;

TEST(Sort, SortKnownIntegers) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            std::default_random_engine generator({ std::random_device()() });
            std::uniform_int_distribution<int> distribution(1, 10000);

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> int {
                    return index;
                },
                100);

            auto sorted = integers.Sort();

            std::vector<int> out_vec;

            sorted.AllGather(&out_vec);

            for (size_t i = 0; i < out_vec.size() - 1; i++) {
                ASSERT_EQ((int)i, out_vec[i]);
            }

            ASSERT_EQ(100u, out_vec.size());
        };

    c7a::api::RunLocalTests(start_func);
}

TEST(Sort, SortRandomIntegers) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            std::default_random_engine generator({ std::random_device()() });
            std::uniform_int_distribution<int> distribution(1, 10000);

            auto integers = Generate(
                ctx,
                [&distribution, &generator](const size_t&) -> int {
                    return distribution(generator);
                },
                100);

            auto sorted = integers.Sort();

            std::vector<int> out_vec;

            sorted.AllGather(&out_vec);

            for (size_t i = 0; i < out_vec.size() - 1; i++) {
                ASSERT_FALSE(out_vec[i + 1] < out_vec[i]);
            }

            ASSERT_EQ(100u, out_vec.size());
        };

    c7a::api::RunLocalTests(start_func);
}

TEST(Sort, SortRandomIntegersCustomCompareFunction) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            std::default_random_engine generator({ std::random_device()() });
            std::uniform_int_distribution<int> distribution(1, 10000);

            auto integers = Generate(
                ctx,
                [&distribution, &generator](const size_t&) -> int {
                    return distribution(generator);
                },
                100);

            auto compare_fn = [](int in1, int in2) {
                                  return in1 > in2;
                              };

            auto sorted = integers.Sort(compare_fn);

            std::vector<int> out_vec;

            sorted.AllGather(&out_vec);

            for (size_t i = 0; i < out_vec.size() - 1; i++) {
                ASSERT_FALSE(out_vec[i + 1] > out_vec[i]);
            }

            ASSERT_EQ(100u, out_vec.size());
        };

    c7a::api::RunLocalTests(start_func);
}

TEST(Sort, SortRandomIntIntStructs) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            using Pair = std::pair<int, int>;

            std::default_random_engine generator({ std::random_device()() });
            std::uniform_int_distribution<int> distribution(1, 10);

            auto integers = Generate(
                ctx,
                [&distribution, &generator](const size_t&) -> Pair {
                    return std::make_pair(distribution(generator), distribution(generator));
                },
                100);

            auto compare_fn = [](Pair in1, Pair in2) {
                                  return in1.first < in2.first;
                              };

            auto sorted = integers.Sort(compare_fn);

            std::vector<Pair> out_vec;

            sorted.AllGather(&out_vec);

            for (size_t i = 0; i < out_vec.size() - 1; i++) {
                ASSERT_FALSE(out_vec[i + 1].first < out_vec[i].first);
            }

            ASSERT_EQ(100u, out_vec.size());
        };

    c7a::api::RunLocalTests(start_func);
}

/******************************************************************************/
