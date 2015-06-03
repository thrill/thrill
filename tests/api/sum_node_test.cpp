/*******************************************************************************
 * tests/api/sum_node_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/dia_base.hpp>
#include <c7a/net/endpoint.hpp>
#include <c7a/core/job_manager.hpp>
#include <c7a/core/stage_builder.hpp>
#include <c7a/api/dia.hpp>
#include <tests/c7a_tests.hpp>

#include "gtest/gtest.h"

using namespace c7a::core;
using namespace c7a::net;

TEST(SumNode, SumNodeExample) {
    using c7a::Context;

    Context ctx;
    std::vector<std::string> self = { "127.0.0.1:1234" };
    ctx.job_manager().Connect(0, Endpoint::ParseEndpointList(self));
    ctx.job_manager().StartDispatcher();

    auto map_fn = [](int in) {
        std::cout << 2 * in << std::endl;
        return 2 * in;
    };

    auto input = ReadFromFileSystem(
            ctx,
            g_workpath + "/inputs/test1",
            [](const std::string& line) {
                return std::stoi(line);
            });

    auto ints = input.Map(map_fn);

    auto sum_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    auto result = ints.Sum(sum_fn);

    ASSERT_EQ(272, result);

    ctx.job_manager().StopDispatcher();
}

/******************************************************************************/
