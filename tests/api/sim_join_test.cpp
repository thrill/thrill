/*******************************************************************************
 * tests/api/sort_node_test.cpp
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

            std::default_random_engine generator(std::random_device { } ());
            std::uniform_int_distribution<int> distribution(1, 10000);

            auto integers1 = Generate(
                ctx,
                [&distribution, &generator](const size_t&) -> int {
                    return distribution(generator);
                },
                10);

			auto integers2 = Generate(
				ctx,
				[&distribution, &generator](const size_t&) -> int {
					return distribution(generator);
				},
				10);

			auto dist_fn = [] (int i1, int i2) -> int {
				return std::abs(i1 - i2);
			};

			auto join_pairs = integers1.TrivialSimJoin(integers2, dist_fn);

            std::vector<std::pair<int, int>> out_vec = join_pairs.AllGather();

			/* for (size_t i = 0; i < out_vec.size() - 1; i++) {
                ASSERT_FALSE(out_vec[i + 1] < out_vec[i]);
            }

            ASSERT_EQ(10000u, out_vec.size());*/
	  };

	  api::RunLocalTests(start_func);
}


/******************************************************************************/
