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

            std::sort(out_vec.begin(), out_vec.end(), [](intuple in1, intuple in2) {
                          return std::get<0>(in1) < std::get<0>(in2);
                      });

            ASSERT_EQ(n, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); i++) {
                ASSERT_EQ(std::make_tuple(i, i * i, i * i * i), out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Join, PairsSameKey) {

    auto start_func =
        [](Context& ctx) {

            using intpair = std::pair<size_t, size_t>;

            size_t n = 333;

            auto dia1 = Generate(ctx, [](const size_t& e) {
                                     return std::make_pair(1, e);
                                 }, n);

            auto dia2 = Generate(ctx, [](const size_t& e) {
                                     return std::make_pair(1, e * e);
                                 }, n);

            auto key_ex = [](intpair input) {
                              return input.first;
                          };

            auto join_fn = [](intpair input1, intpair input2) {
                               return std::make_pair(input1.second, input2.second);
                           };

            auto joined = dia1.InnerJoinWith(dia2, key_ex, key_ex, join_fn);
            std::vector<intpair> out_vec = joined.AllGather();

            std::sort(out_vec.begin(), out_vec.end(), [](intpair in1, intpair in2) {
                          if (in1.first == in2.first) {
                              return in1.second < in2.second;
                          }
                          else {
                              return in1.first < in2.first;
                          }
                      });

            ASSERT_EQ(n * n, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); i++) {
                ASSERT_EQ(std::make_pair(i / n, (i % n) * (i % n)), out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Join, PairsSameKeyDiffSizes) {

    auto start_func =
        [](Context& ctx) {

            using intpair = std::pair<size_t, size_t>;

            size_t n = 333;
			size_t m = 100;

            auto dia1 = Generate(ctx, [](const size_t& e) {
                                     return std::make_pair(1, e);
                                 }, m);

            auto dia2 = Generate(ctx, [](const size_t& e) {
                                     return std::make_pair(1, e * e);
                                 }, n);

            auto key_ex = [](intpair input) {
                              return input.first;
                          };

            auto join_fn = [](intpair input1, intpair input2) {
                               return std::make_pair(input1.second, input2.second);
                           };

            auto joined = dia1.InnerJoinWith(dia2, key_ex, key_ex, join_fn);
            std::vector<intpair> out_vec = joined.AllGather();

            std::sort(out_vec.begin(), out_vec.end(), [](intpair in1, intpair in2) {
                          if (in1.first == in2.first) {
                              return in1.second < in2.second;
                          }
                          else {
                              return in1.first < in2.first;
                          }
                      });

            ASSERT_EQ(n * m, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); i++) {
                ASSERT_EQ(std::make_pair(i / n, (i % n) * (i % n)), out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Join, DifferentTypes) {

    auto start_func =
        [](Context& ctx) {

            using intpair = std::pair<size_t, size_t>;
            using intuple3 = std::tuple<size_t, size_t, size_t>;
			using intuple5 = std::tuple<size_t, size_t, size_t, size_t, size_t>;

            size_t n = 9999;

            auto dia1 = Generate(ctx, [](const size_t& e) {
					                 return std::make_pair(e, e * e);
                                 }, n);

            auto dia2 = Generate(ctx, [](const size_t& e) {
				                    return std::make_tuple(e, e * e, e * e * e);
                                 }, n);

            auto key_ex1 = [](intpair input) {
                              return input.first;
			};
			
			auto key_ex2 = [](intuple3 input) {
				return std::get<0>(input);
			};

            auto join_fn = [](intpair input1, intuple3 input2) {
				              return std::make_tuple(input1.first, input1.second, std::get<0>(input2), std::get<1>(input2), std::get<2>(input2));
                           };

            auto joined = dia1.InnerJoinWith(dia2, key_ex1, key_ex2, join_fn);
            std::vector<intuple5> out_vec = joined.AllGather();

            std::sort(out_vec.begin(), out_vec.end(), [](intuple5 in1, intuple5 in2) {
                          return std::get<0>(in1) < std::get<0>(in2);
                      });

            ASSERT_EQ(n, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); i++) {
                ASSERT_EQ(std::make_tuple(i, i * i, i, i * i, i * i * i), out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
