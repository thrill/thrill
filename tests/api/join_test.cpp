/*******************************************************************************
 * tests/api/join_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/all_gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/join.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/common/logger.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

static constexpr bool debug = false;

TEST(Join, PairsUnique) {

    auto start_func =
        [](Context& ctx) {

		using intpair = std::pair<size_t, size_t>;
		using intuple = std::tuple<size_t, size_t, size_t>;

		size_t n = 9999;

		auto dia1 = Generate(ctx, [](const size_t& e) {
				return std::make_pair(e, e * e);
			}, n);

		auto dia2 = Generate(ctx, [](const size_t& e) {
				return std::make_pair(e, e * e * e);
			}, n);

		auto key_ex = [](intpair input) {
			return input.first;
		};

		auto join_fn = [](intpair input1, intpair input2) {
			return std::make_tuple(input1.first, input1.second, input2.second);
		};

		auto joined = dia1.InnerJoinWith(dia2, key_ex, key_ex, join_fn);
		std::vector<intuple> out_vec = joined.AllGather();

        };

    api::RunLocalTests(start_func);
}


/******************************************************************************/
