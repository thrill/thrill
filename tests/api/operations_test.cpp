/*******************************************************************************
 * tests/api/operations_test.cpp
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
#include <c7a/api/bootstrap.hpp>

#include <algorithm>
#include <random>

#include "gtest/gtest.h"

using namespace c7a::core;
using namespace c7a::net;

TEST(Operations, GeneratorTest) {
    using c7a::Context;

    Context ctx;
    std::vector<std::string> self = { "127.0.0.1:1234" };
    ctx.job_manager().Connect(0, Endpoint::ParseEndpointList(self));

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(1000, 10000);

    size_t generate_size = distribution(generator);

    auto input = GenerateFromFile(
        ctx,
        "test1",
        [](const std::string& line) {
            return std::stoi(line);
        },
        generate_size);

    size_t writer_size = 0;

    input.WriteToFileSystem("test1.out",
                           [&writer_size](const int& item) {
                                //file contains ints between 1  and 15
                                //fails if wrong integer is generated
                                EXPECT_GE(item, 1);
                                EXPECT_GE(16, item);
                                writer_size++;
                                return std::to_string(item);
                           });

    ASSERT_EQ(generate_size, writer_size);
}

TEST(Operations, AllGather) {        

    size_t workers = 4;
    size_t port_base = 8080;

    std::vector<int> vec;

    std::function<void(c7a::Context&)> start_func = [](c7a::Context& ctx) {

        auto integers = ReadLines(
            ctx,
            "test1",
            [](const std::string& line) {
                return std::stoi(line);
            });

        std::vector<int> out_vec;

        integers.AllGather(&out_vec);

        std::sort(out_vec.begin(), out_vec.end());

        int i = 1;
        for (int element : out_vec) {
            ASSERT_EQ(element, i++);
        }

        long unsigned int sixteen = 16;

        ASSERT_EQ(sixteen, out_vec.size());        
    };

    c7a::ExecuteThreads(workers, port_base, start_func);
    

}

/******************************************************************************/
