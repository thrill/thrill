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
#include <c7a/api/reduce_node.hpp>
#include <c7a/api/sum_node.hpp>
#include <c7a/api/bootstrap.hpp>

#include <algorithm>
#include <random>
#include <string>

#include "gtest/gtest.h"

using namespace c7a::core;
using namespace c7a::net;

TEST(Operations, GenerateFromFileCorrectAmountOfCorrectIntegers) {
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
            std::cout << line << std::endl;
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

TEST(Operations, ReadAndAllGatherElementsCorrect) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(2, 4);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(c7a::Context&)> start_func = [](c7a::Context& ctx) {

        auto integers = ReadLines(
            ctx,
            "test1",
            [](const std::string& line) {
                try {
                    return std::stoi(line);
                } catch (const std::invalid_argument&) {
                    return 0;
                }
            });

        std::vector<int> out_vec;

        integers.AllGather(&out_vec);

        std::sort(out_vec.begin(), out_vec.end());

        int i = 1;
        for (int element : out_vec) {
            ASSERT_EQ(element, i++);
        }

        ASSERT_EQ((size_t) 16, out_vec.size());
    };

    c7a::ExecuteThreads(workers, port_base, start_func);

}

TEST(Operations, MapResultsCorrectChangingType) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(2, 4);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(c7a::Context&)> start_func = [](c7a::Context& ctx) {

        auto integers = ReadLines(
            ctx,
            "test1",
            [](const std::string& line) {
                return std::stoi(line);
            });

        std::function<double(int)> double_elements = [](int in) {
            return (double) 2 * in;
        };

        auto doubled = integers.Map(double_elements);

        std::vector<double> out_vec;

        doubled.AllGather(&out_vec);

        std::sort(out_vec.begin(), out_vec.end());

        int i = 1;
        for (int element : out_vec) {
            ASSERT_DOUBLE_EQ(element, (i++ * 2));
        }

        ASSERT_EQ((size_t) 16, out_vec.size());
    };

    c7a::ExecuteThreads(workers, port_base, start_func);

}

TEST(Operations, FlatMapResultsCorrectChangingType) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(2, 4);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(c7a::Context&)> start_func = [](c7a::Context& ctx) {

        auto integers = ReadLines(
            ctx,
            "test1",
            [](const std::string& line) {
                return std::stoi(line);
            });

        auto flatmap_double = [](int in, auto emit) {
            emit((double) 2 * in);
            emit((double) 2 * (in + 16));
        };

        auto doubled = integers.FlatMap(flatmap_double);

        std::vector<int> out_vec;

        doubled.AllGather(&out_vec);

        std::sort(out_vec.begin(), out_vec.end());

        int i = 1;
        for (int element : out_vec) {
            ASSERT_DOUBLE_EQ(element, (i++ * 2));
        }

        ASSERT_EQ((size_t) 32, out_vec.size());
    };

    c7a::ExecuteThreads(workers, port_base, start_func);

}

TEST(Operations, FilterResultsCorrectly) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(2, 4);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(c7a::Context&)> start_func = [](c7a::Context& ctx) {

        auto integers = ReadLines(
            ctx,
            "test1",
            [](const std::string& line) {
                return std::stoi(line);
            });

        std::function<double(int)> even = [](int in) {
            return (in % 2 == 0);
        };

        auto doubled = integers.Filter(even);

        std::vector<int> out_vec;

        doubled.AllGather(&out_vec);

        std::sort(out_vec.begin(), out_vec.end());

        int i = 1;
        for (int element : out_vec) {
            ASSERT_DOUBLE_EQ(element, (i++ * 2));
        }

        ASSERT_EQ((size_t) 8, out_vec.size());
    };

    c7a::ExecuteThreads(workers, port_base, start_func);

}

TEST(Operations, ReduceModulo2CorrectResults) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(2, 3);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(c7a::Context&)> start_func = [](c7a::Context& ctx) {

        auto integers = ReadLines(
            ctx,
            "test1",
            [](const std::string& line) {
                return std::stoi(line);
            });

        auto modulo_two = [](int in) {
            return (in % 2);
        };

        auto add_function = [](int in1, int in2) {
            return in1 + in2;
        };

        auto reduced = integers.ReduceBy(modulo_two, add_function);


        std::vector<int> out_vec;

         std::cout << "starting" << std::endl;

        reduced.AllGather(&out_vec);

        std::cout << "testing" << std::endl;

        std::sort(out_vec.begin(), out_vec.end());

        int i = 1;
        for (int element : out_vec) {
            ASSERT_EQ(element, 56 + (8 * i++));
        }

        ASSERT_EQ((size_t) 2, out_vec.size());
    };

    c7a::ExecuteThreads(workers, port_base, start_func);

}

TEST(Operations, DISABLED_GenerateAndSumHaveEqualAmount) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(2, 4);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::uniform_int_distribution<int> distribution2(1000, 10000);

    size_t generate_size = distribution2(generator);



    std::function<void(c7a::Context&)> start_func = [generate_size](c7a::Context& ctx) {

        auto input = GenerateFromFile(
        ctx,
        "test1",
        [](const std::string& line) {
            return std::stoi(line);
        },
        generate_size);

        auto ones = input.Map([](int){
                return 1;
            });


        auto add_function = [](int in1, int in2) {
            return in1 + in2;
        };

        ASSERT_EQ((int) generate_size, ones.Sum(add_function));
    };

    c7a::ExecuteThreads(workers, port_base, start_func);

}

/******************************************************************************/
