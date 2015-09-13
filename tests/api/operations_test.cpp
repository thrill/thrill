/*******************************************************************************
 * tests/api/operations_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/allgather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/distribute.hpp>
#include <thrill/api/distribute_from.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/generate_from_file.hpp>
#include <thrill/api/prefixsum.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/write_lines_many.hpp>

#include <algorithm>
#include <functional>
#include <random>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

TEST(Operations, DistributeAndAllGatherElements) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            static const size_t test_size = 1024;

            std::vector<size_t> in_vector;

            // generate data everywhere
            for (size_t i = 0; i < test_size; ++i) {
                in_vector.push_back(i);
            }

            // "randomly" shuffle.
            std::default_random_engine gen(123456);
            std::shuffle(in_vector.begin(), in_vector.end(), gen);

            DIARef<size_t> integers = Distribute(ctx, in_vector).Collapse();

            std::vector<size_t> out_vec = integers.AllGather();

            std::sort(out_vec.begin(), out_vec.end());

            ASSERT_EQ(test_size, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); ++i) {
                ASSERT_EQ(i, out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, DistributeFromAndAllGatherElements) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            static const size_t test_size = 1024;

            std::vector<size_t> in_vector;

            if (ctx.my_rank() == 0) {
                // generate data only on worker 0.
                for (size_t i = 0; i < test_size; ++i) {
                    in_vector.push_back(i);
                }

                std::random_shuffle(in_vector.begin(), in_vector.end());
            }

            DIARef<size_t> integers = DistributeFrom(ctx, in_vector, 0).Collapse();

            std::vector<size_t> out_vec = integers.AllGather();

            std::sort(out_vec.begin(), out_vec.end());

            ASSERT_EQ(test_size, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); ++i) {
                ASSERT_EQ(i, out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, DistributeAndGatherElements) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            static const size_t test_size = 1024;

            std::vector<size_t> in_vector;

            // generate data everywhere
            for (size_t i = 0; i < test_size; ++i) {
                in_vector.push_back(i);
            }

            // "randomly" shuffle.
            std::default_random_engine gen(123456);
            std::shuffle(in_vector.begin(), in_vector.end(), gen);

            DIARef<size_t> integers = Distribute(ctx, in_vector).Cache();

            std::vector<size_t> out_vec = integers.Gather(0);

            std::sort(out_vec.begin(), out_vec.end());

            if (ctx.my_rank() == 0) {
                ASSERT_EQ(test_size, out_vec.size());
                for (size_t i = 0; i < out_vec.size(); ++i) {
                    ASSERT_EQ(i, out_vec[i]);
                }
            }
            else {
                ASSERT_EQ(0u, out_vec.size());
            }
        };

    api::RunLocalTests(start_func);
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

    api::RunLocalTests(start_func);
}

TEST(Operations, MapResultsCorrectChangingType) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> size_t {
                    return index + 1;
                },
                16);

            std::function<double(size_t)> double_elements =
                [](size_t in) {
                    return static_cast<double>(2.0 * in);
                };

            auto doubled = integers.Map(double_elements);

            std::vector<double> out_vec = doubled.AllGather();

            size_t i = 1;
            for (double& element : out_vec) {
                ASSERT_DOUBLE_EQ(element, (2.0 * static_cast<double>(i++)));
            }

            ASSERT_EQ(16u, out_vec.size());
            static_assert(std::is_same<decltype(doubled)::ValueType, double>::value, "DIA must be double");
            static_assert(std::is_same<decltype(doubled)::StackInput, size_t>::value, "Node must be size_t");
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, FlatMapResultsCorrectChangingType) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> size_t {
                    return index;
                },
                16);

            auto flatmap_double =
                [](size_t in, auto emit) {
                    emit(static_cast<double>(2 * in));
                    emit(static_cast<double>(2 * (in + 16)));
                };

            auto doubled = integers.FlatMap<double>(flatmap_double);

            std::vector<double> out_vec = doubled.AllGather();

            ASSERT_EQ(32u, out_vec.size());

            for (size_t i = 0; i != out_vec.size() / 2; ++i) {
                ASSERT_DOUBLE_EQ(out_vec[2 * i + 0], 2.0 * i);
                ASSERT_DOUBLE_EQ(out_vec[2 * i + 1], 2.0 * (i + 16));
            }

            static_assert(
                std::is_same<decltype(doubled)::ValueType, double>::value,
                "DIA must be double");

            static_assert(
                std::is_same<decltype(doubled)::StackInput, size_t>::value,
                "Node must be size_t");
        };

    api::RunLocalTests(start_func);
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

    api::RunLocalTests(start_func);
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

    api::RunLocalTests(start_func);
}

TEST(Operations, FilterResultsCorrectly) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return (size_t)index + 1;
                },
                16);

            std::function<bool(size_t)> even = [](size_t in) {
                                                   return (in % 2 == 0);
                                               };

            auto doubled = integers.Filter(even);

            std::vector<size_t> out_vec = doubled.AllGather();

            size_t i = 1;

            for (size_t element : out_vec) {
                ASSERT_DOUBLE_EQ(element, (i++ *2));
            }

            ASSERT_EQ(8u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, DIARefCasting) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto even = [](size_t in) {
                            return (in % 2 == 0);
                        };

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return index + 1;
                },
                16);

            DIARef<size_t> doubled = integers.Filter(even).Collapse();

            std::vector<size_t> out_vec = doubled.AllGather();

            size_t i = 1;

            for (size_t element : out_vec) {
                ASSERT_DOUBLE_EQ(element, (i++ *2));
            }

            ASSERT_EQ(8u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, ForLoop) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> size_t {
                    return index;
                },
                16);

            auto flatmap_duplicate = [](size_t in, auto emit) {
                                         emit(in);
                                         emit(in);
                                     };

            auto map_multiply = [](size_t in) {
                                    return 2 * in;
                                };

            DIARef<size_t> squares = integers.Collapse();

            // run loop four times, inflating DIA of 16 items -> 256
            for (size_t i = 0; i < 4; ++i) {
                auto pairs = squares.FlatMap(flatmap_duplicate);
                auto multiplied = pairs.Map(map_multiply);
                squares = multiplied.Cache();
            }

            std::vector<size_t> out_vec = squares.AllGather();

            ASSERT_EQ(256u, out_vec.size());
            for (size_t i = 0; i != 256; ++i) {
                ASSERT_EQ(out_vec[i], 16 * (i / 16));
            }
            ASSERT_EQ(256u, squares.Size());
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, WhileLoop) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> size_t {
                    return index;
                },
                16);

            auto flatmap_duplicate = [](size_t in, auto emit) {
                                         emit(in);
                                         emit(in);
                                     };

            auto map_multiply = [](size_t in) {
                                    return 2 * in;
                                };

            DIARef<size_t> squares = integers.Collapse();
            size_t sum = 0;

            // run loop four times, inflating DIA of 16 items -> 256
            while (sum < 256) {
                auto pairs = squares.FlatMap(flatmap_duplicate);
                auto multiplied = pairs.Map(map_multiply);
                squares = multiplied.Cache();
                sum = squares.Size();
            }

            std::vector<size_t> out_vec = squares.AllGather();

            ASSERT_EQ(256u, out_vec.size());
            for (size_t i = 0; i != 256; ++i) {
                ASSERT_EQ(out_vec[i], 16 * (i / 16));
            }
            ASSERT_EQ(256u, squares.Size());
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
