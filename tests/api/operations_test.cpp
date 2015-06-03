/*******************************************************************************
 * tests/api/operations_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/dia_base.hpp>
#include <c7a/net/endpoint.hpp>
#include <c7a/core/job_manager.hpp>
#include <c7a/core/stage_builder.hpp>
#include <c7a/api/dia.hpp>
#include <tests/c7a_tests.hpp>

#include <random>

#include "gtest/gtest.h"

using namespace c7a::core;
using namespace c7a::net;

TEST(Operations, GeneratorTest) {
    using c7a::Context;

    Context ctx;
    std::vector<std::string> self = { "127.0.0.1:1234" };
    ctx.job_manager().Connect(0, Endpoint::ParseEndpointList(self));

    auto map_fn = [](int in) {
                      return 2 * in;
                  };

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(1000, 10000);

    size_t generate_size = distribution(generator);

    auto input = GenerateFromFile(
        ctx,
        g_workpath + "/inputs/test1",
        [](const std::string& line) {
            return std::stoi(line);
        },
        generate_size);
    auto ints = input.Map(map_fn);

    size_t writer_size = 0;

    ints.WriteToFileSystem(g_workpath + "/outputs/test1.out",
                           [&writer_size](const int& item) {
                               writer_size++;
                               return std::to_string(item);
                           });

    ASSERT_EQ(generate_size, writer_size);
}

/******************************************************************************/
