/*******************************************************************************
 * tests/api/reduce_node_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/allgather.hpp>
#include <c7a/api/bootstrap.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/api/generate.hpp>
#include <c7a/api/reduce.hpp>
#include <c7a/api/reduce_to_index.hpp>

#include <algorithm>
#include <string>
#include <vector>

#include "gtest/gtest.h"

using c7a::api::Context;
using c7a::api::DIARef;

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

            auto reduced = integers.ReduceBy(modulo_two, add_function);

            std::vector<size_t> out_vec = reduced.AllGather();

            std::sort(out_vec.begin(), out_vec.end());

            int i = 1;

            for (int element : out_vec) {
                ASSERT_EQ(element, 56 + (8 * i++));
            }

            ASSERT_EQ((size_t)2, out_vec.size());
        };

    c7a::api::ExecuteLocalTests(start_func);
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

            size_t max_index = 8;

            auto reduced = integers.ReduceToIndex(key, add_function, max_index);

            std::vector<size_t> out_vec = reduced.AllGather();

            std::sort(out_vec.begin(), out_vec.end());

            int i = 0;
            for (int element : out_vec) {
                switch (i++) {
                case 0:
                    ASSERT_EQ(1, element);
                    break;
                case 1:
                    ASSERT_EQ(5, element);
                    break;
                case 2:
                    ASSERT_EQ(9, element);
                    break;
                case 3:
                    ASSERT_EQ(13, element);
                    break;
                case 4:
                    ASSERT_EQ(16, element);
                    break;
                case 5:
                    ASSERT_EQ(17, element);
                    break;
                case 6:
                    ASSERT_EQ(21, element);
                    break;
                case 7:
                    ASSERT_EQ(25, element);
                    break;
                case 8:
                    ASSERT_EQ(29, element);
                    break;
                default:
                    ASSERT_EQ(42, 420);
                }
            }

            ASSERT_EQ((size_t)9, out_vec.size());
        };

    c7a::api::ExecuteLocalTests(start_func);
}

/******************************************************************************/
