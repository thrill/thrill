/*******************************************************************************
 * tests/api/reduce_node_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/allgather.hpp>
#include <c7a/api/bootstrap.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/api/generate.hpp>
#include <c7a/api/sort.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

using c7a::api::Context;
using c7a::api::DIARef;



TEST(Sort, SortRandomIntegers) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

		
		std::random_device random_device;
		std::default_random_engine generator(random_device());
		std::uniform_int_distribution<int> distribution(1, 10000);

		auto integers = Generate(
			ctx,
			[&distribution, &generator](const size_t&) -> int {
				int toret = distribution(generator);
				return toret;
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

	c7a::api::ExecuteLocalTests(start_func);
}

TEST(Sort, SortRandomIntegersCustomCompareFunction) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

		
		std::random_device random_device;
		std::default_random_engine generator(random_device());
		std::uniform_int_distribution<int> distribution(1, 10000);

		auto integers = Generate(
			ctx,
			[&distribution, &generator](const size_t&) -> int {
				int toret = distribution(generator);
				return toret;
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

	c7a::api::ExecuteLocalTests(start_func);
}

TEST(Sort, SortRandomIntIntStructs) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

		using Pair = std::pair<int, int>;

		std::random_device random_device;
		std::default_random_engine generator(random_device());
		std::uniform_int_distribution<int> distribution(1, 10);

		auto integers = Generate(
			ctx,
			[&distribution, &generator](const size_t&) -> Pair {
				int ele1 = distribution(generator);
				int ele2 = distribution(generator);
				return std::make_pair(ele1, ele2);
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

	c7a::api::ExecuteLocalTests(start_func);
}
