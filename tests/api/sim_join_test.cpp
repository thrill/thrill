/*******************************************************************************
 * tests/api/sim_join_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/all_gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/trivial_sim_join.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

TEST(TrivialSimJoin, SimJoinIntegers1) {

    auto start_func =
        [](Context& ctx) {

            using IntPair = std::pair<int, int>;

            auto integers1 = Generate(
                ctx,
                [](const size_t& i) -> int {
                    return i;
                },
                1000);

            auto integers2 = Generate(
                ctx,
                [](const size_t& i) -> int {
                    return i;
                },
                1000);

            auto arithmetic_distance = [](int i1, int i2) -> int {
                                           return std::abs(i1 - i2);
                                       };

            auto sort_pairs = [](IntPair ip1, IntPair ip2) {
                                  if (ip1.first == ip2.first) {
                                      return ip1.second < ip2.second;
                                  }
                                  else {
                                      return ip1.first < ip2.first;
                                  }
                              };

            int similarity_threshhold = 2;

            auto joined_pairs = integers1.TrivialSimJoin(integers2,
                                                         arithmetic_distance,
                                                         similarity_threshhold);

            auto sorted_joined_pairs = joined_pairs.Sort(sort_pairs);

            // If everythign worked out correctly, this vector contains all pairs of integers
            // between 0 and 999, in which the first and second integer differ by less than 2
            // [0,0], [0,1], [1,0], [1,1], [1,2] ...
            std::vector<std::pair<int, int> > out_vec = sorted_joined_pairs.AllGather();

            ASSERT_EQ(2998u, out_vec.size());

            for (unsigned int i = 0; i < out_vec.size(); i++) {

                int expected_first = (i + 1) / 3;
                ASSERT_EQ(expected_first, out_vec[i].first);

                int expected_second = (((i + 1) % 3) - 1) + expected_first;
                ASSERT_EQ(expected_second, out_vec[i].second);
            }
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
