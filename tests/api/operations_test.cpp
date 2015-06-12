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
        "test1",
        [](const std::string& line) {
            return std::stoi(line);
        },
        generate_size);
    auto ints = input.Map(map_fn);

    size_t writer_size = 0;

    ints.WriteToFileSystem("test1.out",
                           [&writer_size](const int& item) {
                               writer_size++;
                               return std::to_string(item);
                           });

    ASSERT_EQ(generate_size, writer_size);
}

std::vector<int> all_gather_test_function(c7a::Context& ctx) {
    using c7a::Context;

    auto integers = ReadLines(
        ctx,
        "test1",
        [](const std::string& line) {
            return std::stoi(line);
        });

    std::vector<int> out_vec;

    integers.AllGather(&out_vec);

    std::cout << out_vec.size() << std::endl;

    return out_vec;
}

TEST(Operations, AllGather) {        

    size_t workers = 4;
    size_t port_base = 8080;

    std::vector<int> vec;

    std::function<int(c7a::Context&)> start_func = [](c7a::Context& ctx) {

        auto integers = ReadLines(
            ctx,
            "test1",
            [](const std::string& line) {
                return std::stoi(line);
            });

        std::vector<int> out_vec;

        integers.AllGather(&out_vec);

        //ASSERT_EQ(15, out_vec.size());

        return out_vec.size();
        
    };

    
    //ASSERT_EQ(15, vec.size());

    c7a::ExecuteThreads(workers, port_base, start_func);
    

}

/******************************************************************************/
