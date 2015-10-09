/*******************************************************************************
 * tests/api/sort_node_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/allgather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/sort.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

TEST(Sort, SortKnownIntegers) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            std::default_random_engine generator(std::random_device { } ());
            std::uniform_int_distribution<int> distribution(1, 10000);

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> size_t {
                    return index;
                },
                10000);

            auto sorted = integers.Sort();

            std::vector<size_t> out_vec;

            sorted.AllGather(&out_vec);

            for (size_t i = 0; i < out_vec.size() - 1; i++) {
                ASSERT_EQ(i, out_vec[i]);
            }

            ASSERT_EQ(10000u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(Sort, SortRandomIntegers) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            std::default_random_engine generator(std::random_device { } ());
            std::uniform_int_distribution<int> distribution(1, 10000);

            auto integers = Generate(
                ctx,
                [&distribution, &generator](const size_t&) -> int {
                    return distribution(generator);
                },
                10000);

            auto sorted = integers.Sort();

            std::vector<int> out_vec;

            sorted.AllGather(&out_vec);

            for (size_t i = 0; i < out_vec.size() - 1; i++) {
                ASSERT_FALSE(out_vec[i + 1] < out_vec[i]);
            }

            ASSERT_EQ(10000u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(Sort, SortRandomIntegersCustomCompareFunction) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            std::default_random_engine generator(std::random_device { } ());
            std::uniform_int_distribution<int> distribution(1, 10000);

            auto integers = Generate(
                ctx,
                [&distribution, &generator](const size_t&) -> int {
                    return distribution(generator);
                },
                10000);

            auto compare_fn = [](int in1, int in2) {
                                  return in1 > in2;
                              };

            auto sorted = integers.Sort(compare_fn);

            std::vector<int> out_vec;

            sorted.AllGather(&out_vec);

            for (size_t i = 0; i < out_vec.size() - 1; i++) {
                ASSERT_FALSE(out_vec[i + 1] > out_vec[i]);
            }

            ASSERT_EQ(10000u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(Sort, SortRandomIntIntStructs) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            using Pair = std::pair<int, int>;

            std::default_random_engine generator(std::random_device { } ());
            std::uniform_int_distribution<int> distribution(1, 10);

            auto integers = Generate(
                ctx,
                [&distribution, &generator](const size_t&) -> Pair {
                    return std::make_pair(distribution(generator), distribution(generator));
                },
                10000);

            auto compare_fn = [](Pair in1, Pair in2) {
                                  if (in1.first != in2.first) {
                                      return in1.first < in2.first;
                                  }
                                  else {
                                      return in1.second < in2.second;
                                  }
                              };

            auto sorted = integers.Sort(compare_fn);

            std::vector<Pair> out_vec;

            sorted.AllGather(&out_vec);

            for (size_t i = 0; i < out_vec.size() - 1; i++) {
                ASSERT_FALSE(out_vec[i + 1].first < out_vec[i].first);
            }

            ASSERT_EQ(10000u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(Sort, SortZeros) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {


            std::default_random_engine generator(std::random_device { } ());
            std::uniform_int_distribution<int> distribution(1, 10);

            auto integers = Generate(
                ctx,
                [](const size_t&) -> size_t {
                    return 0;
                },
                10000);

            auto sorted = integers.Sort();

            std::vector<size_t> out_vec;

            sorted.AllGather(&out_vec);

			/* for (size_t i = 0; i < out_vec.size() - 1; i++) {
                ASSERT_FALSE(out_vec[i + 1].first < out_vec[i].first);
				}*/

            ASSERT_EQ(10000u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
