/*******************************************************************************
 * tests/api/operations_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/allgather.hpp>
#include <c7a/api/generate.hpp>
#include <c7a/api/generate_from_file.hpp>
#include <c7a/api/lop_node.hpp>
#include <c7a/api/prefixsum.hpp>
#include <c7a/api/read_lines.hpp>
#include <c7a/api/scatter.hpp>
#include <c7a/api/size.hpp>
#include <c7a/api/write.hpp>
#include <c7a/api/write_single_file.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <random>
#include <string>
#include <vector>

using namespace c7a;
using c7a::api::Context;
using c7a::api::DIARef;

TEST(Operations, GenerateFromFileCorrectAmountOfCorrectIntegers) {

    std::vector<std::string> self = { "127.0.0.1:1234" };
    core::JobManager jobMan;
    jobMan.Connect(0, net::Endpoint::ParseEndpointList(self), 1);
    Context ctx(jobMan, 0);

    std::default_random_engine generator({ std::random_device()() });
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

    input.Map(
        [&writer_size](const int& item) {
            //file contains ints between 1  and 15
            //fails if wrong integer is generated
            EXPECT_GE(item, 1);
            EXPECT_GE(16, item);
            writer_size++;
            return std::to_string(item) + "\n";
        })
    .WriteLinesMany("test1.out");

    ASSERT_EQ(generate_size, writer_size);
}

TEST(Operations, WriteToSingleFile) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = ReadLines(ctx, "test1")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });
            integers.Map(
                [](const int& item) {
                    return std::to_string(item) + "\n";
                })
            .WriteLines("testsf.out");
            
        };

    api::ExecuteLocalTests(start_func);
}

TEST(Operations, ReadAndAllGatherElementsCorrect) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = ReadLines(ctx, "test1")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });

            std::vector<int> out_vec = integers.AllGather();

            int i = 1;
            for (int element : out_vec) {
                ASSERT_EQ(element, i++);
            }

            ASSERT_EQ((size_t)16, out_vec.size());
        };

    api::ExecuteLocalTests(start_func);
}

TEST(Operations, ScatterAndAllGatherElementsCorrect) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            static const size_t test_size = 1024;

            std::vector<size_t> in_vector;

            if (ctx.rank() == 0) {
                // generate data only on worker 0.
                for (size_t i = 0; i < test_size; ++i) {
                    in_vector.push_back(i);
                }

                std::random_shuffle(in_vector.begin(), in_vector.end());
            }

            DIARef<size_t> integers = Scatter(ctx, in_vector).Collapse();

            std::vector<size_t> out_vec = integers.AllGather();

            std::sort(out_vec.begin(), out_vec.end());

            ASSERT_EQ(test_size, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); ++i) {
                ASSERT_EQ(i, out_vec[i]);
            }
        };

    api::ExecuteLocalTests(start_func);
}

TEST(Operations, GenerateIntegers) {

    static const size_t test_size = 1000;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) { return index; },
                test_size);

            std::vector<size_t> out_vec = integers.AllGather();

            ASSERT_EQ(test_size, out_vec.size());

            for (size_t i = 0; i < test_size; ++i) {
                ASSERT_EQ(i, out_vec[i]);
            }
        };

    api::ExecuteLocalTests(start_func);
}

TEST(Operations, MapResultsCorrectChangingType) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> int {
                    return index + 1;
                },
                16);

            std::function<double(int)> double_elements =
                [](int in) {
                    return (double)2 * in;
                };

            auto doubled = integers.Map(double_elements);

            std::vector<double> out_vec = doubled.AllGather();

            int i = 1;
            for (int element : out_vec) {
                ASSERT_DOUBLE_EQ(element, (i++ *2));
            }

            ASSERT_EQ(16u, out_vec.size());
            static_assert(std::is_same<decltype(doubled)::ValueType, double>::value, "DIA must be double");
            static_assert(std::is_same<decltype(doubled)::StackInput, int>::value, "Node must be int");
        };

    api::ExecuteLocalTests(start_func);
}

TEST(Operations, FlatMapResultsCorrectChangingType) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> int {
                    return index;
                },
                16);

            auto flatmap_double = [](int in, auto emit) {
                                      emit((double)2 * in);
                                      emit((double)2 * (in + 16));
                                  };

            auto doubled = integers.FlatMap<double>(flatmap_double);

            std::vector<double> out_vec = doubled.AllGather();

            ASSERT_EQ(32u, out_vec.size());

            for (size_t i = 0; i != out_vec.size() / 2; ++i) {
                ASSERT_DOUBLE_EQ(out_vec[2 * i + 0], 2 * i);
                ASSERT_DOUBLE_EQ(out_vec[2 * i + 1], 2 * (i + 16));
            }

            static_assert(
                std::is_same<decltype(doubled)::ValueType, double>::value,
                "DIA must be double");

            static_assert(
                std::is_same<decltype(doubled)::StackInput, int>::value,
                "Node must be int");
        };

    api::ExecuteLocalTests(start_func);
}

TEST(Operations, PrefixSumCorrectResults) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& input) {
                    return input + 1;
                },
                16);

            auto prefixsums = integers.PrefixSum();

            std::vector<size_t> out_vec = prefixsums.AllGather();

            size_t ctr = 0;
            for (size_t i = 0; i < out_vec.size(); i++) {
                ctr += i + 1;
                ASSERT_EQ(out_vec[i], ctr);
            }

            ASSERT_EQ((size_t)16, out_vec.size());
        };

    api::ExecuteLocalTests(start_func);
}

TEST(Operations, PrefixSumFacultyCorrectResults) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& input) {
                    return input + 1;
                },
                10);

            auto prefixsums = integers.PrefixSum(
                [](size_t in1, size_t in2) {
                    return in1 * in2;
                }, 1);

            std::vector<size_t> out_vec = prefixsums.AllGather();

            size_t ctr = 1;
            for (size_t i = 0; i < out_vec.size(); i++) {
                ctr *= i + 1;
                ASSERT_EQ(out_vec[i], ctr);
            }

            ASSERT_EQ(10u, out_vec.size());
        };

    api::ExecuteLocalTests(start_func);
}

TEST(Operations, FilterResultsCorrectly) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return (int)index + 1;
                },
                16);

            std::function<bool(int)> even = [](int in) {
                                                return (in % 2 == 0);
                                            };

            auto doubled = integers.Filter(even);

            std::vector<int> out_vec = doubled.AllGather();

            int i = 1;

            for (int element : out_vec) {
                ASSERT_DOUBLE_EQ(element, (i++ *2));
            }

            ASSERT_EQ(8u, out_vec.size());
        };

    api::ExecuteLocalTests(start_func);
}

TEST(Operations, DIARefCasting) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto even = [](int in) {
                            return (in % 2 == 0);
                        };

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return (int)index + 1;
                },
                16);

            DIARef<int> doubled = integers.Filter(even).Collapse();

            std::vector<int> out_vec = doubled.AllGather();

            int i = 1;

            for (int element : out_vec) {
                ASSERT_DOUBLE_EQ(element, (i++ *2));
            }

            ASSERT_EQ(8u, out_vec.size());
        };

    api::ExecuteLocalTests(start_func);
}

TEST(Operations, ForLoop) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> int {
                    return index;
                },
                16);

            auto flatmap_duplicate = [](int in, auto emit) {
                                         emit(in);
                                         emit(in);
                                     };

            auto map_multiply = [](int in) {
                                    return 2 * in;
                                };

            DIARef<int> squares = integers.Collapse();

            // run loop four times, inflating DIA of 16 items -> 256
            for (size_t i = 0; i < 4; ++i) {
                auto pairs = squares.FlatMap(flatmap_duplicate);
                auto multiplied = pairs.Map(map_multiply);
                squares = multiplied.Collapse();
            }

            std::vector<int> out_vec = squares.AllGather();

            ASSERT_EQ(256u, out_vec.size());
            for (size_t i = 0; i != 256; ++i) {
                ASSERT_EQ(out_vec[i], (int)(16 * (i / 16)));
            }
            ASSERT_EQ(256u, squares.Size());
        };

    api::ExecuteLocalTests(start_func);
}

TEST(Operations, WhileLoop) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> int {
                    return index;
                },
                16);

            auto flatmap_duplicate = [](int in, auto emit) {
                                         emit(in);
                                         emit(in);
                                     };

            auto map_multiply = [](int in) {
                                    return 2 * in;
                                };

            DIARef<int> squares = integers.Collapse();
            unsigned int sum = 0;

            // run loop four times, inflating DIA of 16 items -> 256
            while (sum < 256) {
                auto pairs = squares.FlatMap(flatmap_duplicate);
                auto multiplied = pairs.Map(map_multiply);
                squares = multiplied.Collapse();
                sum = squares.Size();
            }

            std::vector<int> out_vec = squares.AllGather();

            ASSERT_EQ(256u, out_vec.size());
            for (size_t i = 0; i != 256; ++i) {
                ASSERT_EQ(out_vec[i], (int)(16 * (i / 16)));
            }
            ASSERT_EQ(256u, squares.Size());
        };

    api::ExecuteLocalTests(start_func);
}

/******************************************************************************/
