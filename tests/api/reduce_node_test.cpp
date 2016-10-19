/*******************************************************************************
 * tests/api/reduce_node_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/all_gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

TEST(ReduceNode, ReduceModulo2CorrectResults) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx, 16,
                [](const size_t& index) {
                    return index + 1;
                });

            auto modulo_two = [](size_t in) {
                                  return (in % 4);
                              };

            auto add_function = [](const size_t& in1, const size_t& in2) {
                                    return in1 + in2;
                                };

            auto reduced = integers.ReduceByKey(
                VolatileKeyTag, modulo_two, add_function);

            std::vector<size_t> out_vec = reduced.AllGather();
            ASSERT_EQ(4u, out_vec.size());

            std::sort(out_vec.begin(), out_vec.end());

            size_t i = 1;
            for (const size_t& element : out_vec) {
                ASSERT_EQ(element, 24 + (4 * i++));
            }
        };

    api::RunLocalTests(start_func);
}

//! Test sums of integers 0..n-1 for n=100 in 1000 buckets in the reduce table
TEST(ReduceNode, ReduceModuloPairsCorrectResults) {

    static constexpr size_t test_size = 1000000u;
    static constexpr size_t mod_size = 1000u;
    static constexpr size_t div_size = test_size / mod_size;

    auto start_func =
        [](Context& ctx) {

            using IntPair = std::pair<size_t, size_t>;

            auto integers = Generate(
                ctx, test_size,
                [](const size_t& index) {
                    return IntPair(index % mod_size, index / mod_size);
                });

            auto add_function = [](const size_t& in1, const size_t& in2) {
                                    return in1 + in2;
                                };

            auto reduced = integers.ReducePair(add_function);

            std::vector<IntPair> out_vec = reduced.AllGather();

            std::sort(out_vec.begin(), out_vec.end(),
                      [](const IntPair& p1, const IntPair& p2) {
                          return p1.first < p2.first;
                      });

            ASSERT_EQ(mod_size, out_vec.size());
            for (const auto& element : out_vec) {
                ASSERT_EQ(element.second, (div_size * (div_size - 1)) / 2u);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(ReduceNode, ReduceToIndexCorrectResults) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx, 16,
                [](const size_t& index) {
                    return index + 1;
                });

            auto key = [](size_t in) {
                           return in / 2;
                       };

            auto add_function = [](const size_t& in1, const size_t& in2) {
                                    return in1 + in2;
                                };

            size_t result_size = 9;

            auto reduced = integers.ReduceToIndex(
                VolatileKeyTag, key, add_function, result_size);

            std::vector<size_t> out_vec = reduced.AllGather();
            ASSERT_EQ(9u, out_vec.size());

            size_t i = 0;
            for (size_t element : out_vec) {
                switch (i++) {
                case 0:
                    ASSERT_EQ(1u, element);
                    break;
                case 1:
                    ASSERT_EQ(5u, element);
                    break;
                case 2:
                    ASSERT_EQ(9u, element);
                    break;
                case 3:
                    ASSERT_EQ(13u, element);
                    break;
                case 4:
                    ASSERT_EQ(17u, element);
                    break;
                case 5:
                    ASSERT_EQ(21u, element);
                    break;
                case 6:
                    ASSERT_EQ(25u, element);
                    break;
                case 7:
                    ASSERT_EQ(29u, element);
                    break;
                case 8:
                    ASSERT_EQ(16u, element);
                    break;
                default:
                    ASSERT_EQ(42, 420);
                }
            }
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
