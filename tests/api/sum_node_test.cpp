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
#include <c7a/net/collective_communication.hpp>

#include "gtest/gtest.h"

#include <random>
#include <thread>
#include <string>

#include <c7a/api/bootstrap.hpp>
#include <c7a/api/dia.hpp>

using namespace c7a::core;
using namespace c7a::net;

TEST(SumNode, LocalhostOneThread) {
    using c7a::Context;

    Context ctx;
    std::vector<std::string> self = { "127.0.0.1:1234" };
    ctx.job_manager().Connect(0, Endpoint::ParseEndpointList(self));

    auto map_fn = [](int in) {
                      return 2 * in;
                  };

    auto input = ReadFromFileSystem(
        ctx,
        "test1",
        [](const std::string& line) {
            return std::stoi(line);
        });

    auto ints = input.Map(map_fn);

    auto sum_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    auto result = ints.Sum(sum_fn);

    ASSERT_EQ(272, result);
}

int sum_node_generated(c7a::Context& ctx) {

    auto map_fn = [](int in) {
                      std::cout << 2 * in << std::endl;
                      return 2 * in;
                  };

    auto input = ReadFromFileSystem(
        ctx,
        "test1",
        [](const std::string& line) {
            std::cout << "out: " << line << std::endl;
            return std::stoi(line);
        });

    auto ints = input.Map(map_fn);

    auto sum_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    auto result = ints.Sum(sum_fn);
    //std::cout << result << std::endl;
    return result;
}

TEST(SumNode, DISABLED_LocalhostTwoThreads) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(30000, 65000);
    const size_t port_base = distribution(generator);

    unsigned int workers = 2;

    std::vector<std::thread> threads(workers);
    std::vector<char**> arguments(workers);
    std::vector<std::vector<std::string> > strargs(workers);

    for (size_t i = 0; i < workers; i++) {

        arguments[i] = new char*[workers + 2];
        strargs[i].resize(workers + 2);

        for (size_t j = 0; j < workers; j++) {
            strargs[i][j + 2] += "127.0.0.1:";
            strargs[i][j + 2] += std::to_string(port_base + j);
            arguments[i][j + 2] = const_cast<char*>(strargs[i][j + 2].c_str());
        }

        std::function<int(c7a::Context&)> start_func = [](c7a::Context& ctx) {
                                                           return sum_node_generated(ctx);
                                                       };

        strargs[i][0] = "sum node";
        arguments[i][0] = const_cast<char*>(strargs[i][0].c_str());
        strargs[i][1] = std::to_string(i);
        arguments[i][1] = const_cast<char*>(strargs[i][1].c_str());
        threads[i] = std::thread([=]() { c7a::Execute(workers + 2, arguments[i], start_func); });
    }

    for (size_t i = 0; i < workers; i++) {
        threads[i].join();
    }
}

/******************************************************************************/
