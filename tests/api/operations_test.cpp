/*******************************************************************************
 * tests/api/operations_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

//#include <c7a/api/dia_base.hpp>
#include <c7a/net/endpoint.hpp>
#include <c7a/api/node_include.hpp>
#include <c7a/api/bootstrap.hpp>

#include <algorithm>
#include <random>
#include <string>

#include "gtest/gtest.h"

using namespace c7a::core;
using namespace c7a::net;
using c7a::api::Context;
using c7a::api::DIARef;

TEST(Operations, GenerateFromFileCorrectAmountOfCorrectIntegers) {

    std::vector<std::string> self = { "127.0.0.1:1234" };
    JobManager jobMan;
    jobMan.Connect(0, Endpoint::ParseEndpointList(self), 1);
    Context ctx(jobMan, 0);

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

TEST(Operations, ReadAndAllGatherElementsCorrect) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(1, 4);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(Context&)> start_func = [](Context& ctx) {

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

                                                   ASSERT_EQ((size_t)16, out_vec.size());
                                               };

    c7a::api::ExecuteThreads(workers, port_base, start_func);
}

TEST(Operations, MapResultsCorrectChangingType) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(1, 4);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(Context&)> start_func = [](Context& ctx) {

                                                   auto integers = Generate(
                                                       ctx,
                                                       [](const size_t& input) {
                                                           return input + 1;
                                                       },
                                                       16);

                                                   std::function<double(int)> double_elements = [](int in) {
                                                                                                    return (double)2 * in;
                                                                                                };

                                                   auto doubled = integers.Map(double_elements);

                                                   std::vector<double> out_vec;

                                                   doubled.AllGather(&out_vec);

                                                   std::sort(out_vec.begin(), out_vec.end());

                                                   int i = 1;
                                                   for (int element : out_vec) {
                                                       ASSERT_DOUBLE_EQ(element, (i++ *2));
                                                   }

                                                   ASSERT_EQ((size_t)16, out_vec.size());
                                               };

    c7a::api::ExecuteThreads(workers, port_base, start_func);
}

TEST(Operations, FlatMapResultsCorrectChangingType) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(1, 4);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(Context&)> start_func = [](Context& ctx) {

                                                   auto integers = Generate(
                                                       ctx,
                                                       [](const size_t& input) {
                                                           return input + 1;
                                                       },
                                                       16);

                                                   auto flatmap_double = [](int in, auto emit) {
                                                                             emit((double)2 * in);
                                                                             emit((double)2 * (in + 16));
                                                                         };

                                                   auto doubled = integers.FlatMap(flatmap_double);

                                                   std::vector<int> out_vec;

                                                   doubled.AllGather(&out_vec);

                                                   std::sort(out_vec.begin(), out_vec.end());

                                                   int i = 1;
                                                   for (int element : out_vec) {
                                                       ASSERT_DOUBLE_EQ(element, (i++ *2));
                                                   }

                                                   ASSERT_EQ((size_t)32, out_vec.size());
                                               };

    c7a::api::ExecuteThreads(workers, port_base, start_func);
}

TEST(Operations, PrefixSumCorrectResults) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(1, 4);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(Context&)> start_func = [](Context& ctx) {

                                                   auto integers = Generate(
                                                       ctx,
                                                       [](const size_t& input) {
                                                           return input + 1;
                                                       },
                                                       16);

                                                   auto prefixsums = integers.PrefixSum(
                                                       [](size_t in1, size_t in2) {
                                                           return in1 + in2;
                                                       });

                                                   std::vector<size_t> out_vec;

                                                   prefixsums.AllGather(&out_vec);

                                                   std::sort(out_vec.begin(), out_vec.end());
                                                   size_t ctr = 0;
                                                   for (size_t i = 0; i < out_vec.size(); i++) {
                                                       ctr += i + 1;
                                                       ASSERT_EQ(out_vec[i], ctr);
                                                   }

                                                   ASSERT_EQ((size_t)16, out_vec.size());
                                               };

    c7a::api::ExecuteThreads(workers, port_base, start_func);
}

TEST(Operations, FilterResultsCorrectly) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(1, 4);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(Context&)> start_func = [](Context& ctx) {

                                                   auto integers = Generate(
                                                       ctx,
                                                       [](const size_t& input) {
                                                           return input + 1;
                                                       },
                                                       16);

                                                   std::function<double(int)> even = [](int in) {
                                                                                         return (in % 2 == 0);
                                                                                     };

                                                   auto doubled = integers.Filter(even);

                                                   std::vector<int> out_vec;

                                                   doubled.AllGather(&out_vec);

                                                   std::sort(out_vec.begin(), out_vec.end());

                                                   int i = 1;

                                                   for (int element : out_vec) {
                                                       ASSERT_DOUBLE_EQ(element, (i++ *2));
                                                   }

                                                   ASSERT_EQ((size_t)8, out_vec.size());
                                               };

    c7a::api::ExecuteThreads(workers, port_base, start_func);
}

TEST(Operations, ReduceModulo2CorrectResults) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(1, 4);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(Context&)> start_func = [](Context& ctx) {

                                                   auto integers = Generate(
                                                       ctx,
                                                       [](const size_t& input) {
                                                           return input + 1;
                                                       },
                                                       16);

                                                   auto modulo_two = [](int in) {
                                                                         return (in % 2);
                                                                     };

                                                   auto add_function = [](int in1, int in2) {
                                                                           return in1 + in2;
                                                                       };

                                                   auto reduced = integers.ReduceBy(modulo_two, add_function);

                                                   std::vector<int> out_vec;

                                                   reduced.AllGather(&out_vec);

                                                   std::sort(out_vec.begin(), out_vec.end());

                                                   int i = 1;

                                                   for (int element : out_vec) {
                                                       ASSERT_EQ(element, 56 + (8 * i++));
                                                   }

                                                   ASSERT_EQ((size_t)2, out_vec.size());
                                               };

    c7a::api::ExecuteThreads(workers, port_base, start_func);
}

TEST(Operations, ReduceToIndexCorrectResults) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(1, 4);

    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(Context&)> start_func = [](Context& ctx) {

                                                   auto integers = Generate(
                                                       ctx,
                                                       [](const size_t& input) {
                                                           return input + 1;
                                                       },
                                                       16);

                                                   auto key = [](size_t in) {
                                                                  return in / 2;
                                                              };

                                                   auto add_function = [](int in1, int in2) {
                                                                           return in1 + in2;
                                                                       };

                                                   size_t max_index = 9;

                                                   auto reduced = integers.ReduceToIndex(key, add_function, max_index);

                                                   std::vector<int> out_vec;

                                                   reduced.AllGather(&out_vec);

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

    c7a::api::ExecuteThreads(workers, port_base, start_func);
}

/******************************************************************************/
