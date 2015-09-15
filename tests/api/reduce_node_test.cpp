/*******************************************************************************
 * tests/api/reduce_node_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/allgather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/reduce_to_index.hpp>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

TEST(ReduceNode, ReduceModulo2CorrectResults) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return index + 1;
                },
                16);

            auto modulo_two = [](size_t in) {
                                  return (in % 2);
                              };

            auto add_function = [](const size_t& in1, const size_t& in2) {
                                    return in1 + in2;
                                };

            auto reduced = integers.ReduceByKey(modulo_two, add_function);

            std::vector<size_t> out_vec = reduced.AllGather();

            std::sort(out_vec.begin(), out_vec.end());

            size_t i = 1;

            for (const size_t& element : out_vec) {
                ASSERT_EQ(element, 56 + (8 * i++));
            }

            ASSERT_EQ((size_t)2, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

//! Test sums of integers 0..n-1 for n=100 in 1000 buckets in the reduce table
TEST(ReduceNode, ReduceModulo2PairsCorrectResults) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            using IntPair = std::pair<size_t, size_t>;

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return IntPair(index % 1000, index / 1000);
                },
                100000u);

            auto add_function = [](const size_t& in1, const size_t& in2) {
                                    return in1 + in2;
                                };

            auto reduced = integers.ReducePair(add_function);

            std::vector<IntPair> out_vec = reduced.AllGather();

            std::sort(out_vec.begin(), out_vec.end(),
                      [](const IntPair& p1, const IntPair& p2) {
                          return p1.first < p2.first;
                      });

            for (const auto& element : out_vec) {
                ASSERT_EQ(element.second, (100u * 99u) / 2u);
            }

            ASSERT_EQ(1000u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(ReduceNode, ReducePairToIndexCorrectResults) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return std::make_pair((index + 1) / 2, index + 1);
                },
                16);

            auto add_function = [](const size_t& in1, const size_t& in2) {
                                    return in1 + in2;
                                };

            size_t result_size = 9;

            auto reduced = integers.ReducePairToIndex(add_function, result_size);

            std::vector<std::pair<size_t, size_t> > out_vec = reduced.AllGather();
            ASSERT_EQ(9u, out_vec.size());

            int i = 0;
            for (auto element : out_vec) {
                switch (i++) {
                case 0:
                    ASSERT_EQ(1u, element.second);
                    break;
                case 1:
                    ASSERT_EQ(5u, element.second);
                    break;
                case 2:
                    ASSERT_EQ(9u, element.second);
                    break;
                case 3:
                    ASSERT_EQ(13u, element.second);
                    break;
                case 4:
                    ASSERT_EQ(17u, element.second);
                    break;
                case 5:
                    ASSERT_EQ(21u, element.second);
                    break;
                case 6:
                    ASSERT_EQ(25u, element.second);
                    break;
                case 7:
                    ASSERT_EQ(29u, element.second);
                    break;
                case 8:
                    ASSERT_EQ(16u, element.second);
                    break;
                default:
                    ASSERT_EQ(42, 420);
                }
            }
        };

    api::RunLocalTests(start_func);
}

TEST(ReduceNode, ReduceToIndexCorrectResults) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return index + 1;
                },
                16);

            auto key = [](size_t in) {
                           return in / 2;
                       };

            auto add_function = [](const size_t& in1, const size_t& in2) {
                                    return in1 + in2;
                                };

            size_t result_size = 9;

            auto reduced = integers.ReduceToIndexByKey(key, add_function, result_size);

            std::vector<size_t> out_vec = reduced.AllGather();
            ASSERT_EQ(9u, out_vec.size());

            int i = 0;
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
