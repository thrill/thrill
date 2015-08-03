/*******************************************************************************
 * tests/api/sum_node_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/generate_from_file.hpp>
#include <c7a/api/read.hpp>
#include <c7a/api/sum.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <string>

using namespace c7a::core;
using namespace c7a::net;

using c7a::api::Context;
using c7a::api::DIARef;

TEST(SumNode, GenerateAndSumHaveEqualAmount1) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(1000, 10000);

    size_t generate_size = distribution(generator);

    std::function<void(Context&)> start_func =
        [generate_size](Context& ctx) {

            auto input = GenerateFromFile(
                ctx,
                "test1",
                [](const std::string& line) {
                    return std::stoi(line);
                },
                generate_size);

            auto ones = input.Map([](int) {
                                      return 1;
                                  });

            auto add_function = [](int in1, int in2) {
                                    return in1 + in2;
                                };

            ASSERT_EQ((int)generate_size, ones.Sum(add_function));
        };

    c7a::api::ExecuteLocalTests(start_func);
}

TEST(SumNode, GenerateAndSumHaveEqualAmount2) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // TODO(ms): Replace this with some test-specific rendered file
            auto input = ReadLines(ctx, "test1")
                         .Map([](const std::string& line) {
                                  return std::stoi(line);
                              });

            auto ones = input.Map([](int in) {
                                      return in;
                                  });

            auto add_function = [](int in1, int in2) {
                                    return in1 + in2;
                                };

            ASSERT_EQ(136, ones.Sum(add_function));
        };

    c7a::api::ExecuteLocalTests(start_func);
}

/******************************************************************************/
