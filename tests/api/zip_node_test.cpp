/*******************************************************************************
 * tests/api/zip_node_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/all_gather.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/common/string.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <tuple>
#include <vector>

using namespace thrill; // NOLINT

struct MyStruct {
    int a, b;
};

static constexpr size_t test_size = 1000;

TEST(ZipNode, TwoBalancedIntegerArrays) {

    auto start_func =
        [](Context& ctx) {

            // numbers 0..999 (evenly distributed to workers)
            auto zip_input1 = Generate(
                ctx, test_size,
                [](size_t index) { return index; });

            // numbers 1000..1999
            auto zip_input2 = zip_input1.Map(
                [](size_t i) { return static_cast<short>(test_size + i); });

            // zip
            auto zip_result = zip_input1.Zip(
                zip_input2, [](size_t a, short b) -> long {
                    return static_cast<long>(a) + b;
                });

            // check result
            std::vector<long> res = zip_result.AllGather();

            ASSERT_EQ(test_size, res.size());

            for (size_t i = 0; i != res.size(); ++i) {
                ASSERT_EQ(static_cast<long>(i + i + test_size), res[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(ZipNode, TwoBalancedIntegerArraysNoRebalance) {

    auto start_func =
        [](Context& ctx) {

            // numbers 0..999 (evenly distributed to workers)
            auto zip_input1 = Generate(
                ctx, test_size,
                [](size_t index) { return index; });

            // numbers 1000..1999
            auto zip_input2 = zip_input1.Map(
                [](size_t i) { return static_cast<short>(test_size + i); });

            // zip
            auto zip_result = zip_input1.Zip(
                NoRebalanceTag,
                zip_input2, [](size_t a, short b) -> long {
                    return static_cast<long>(a) + b;
                });

            // check result
            std::vector<long> res = zip_result.AllGather();

            ASSERT_EQ(test_size, res.size());

            for (size_t i = 0; i != res.size(); ++i) {
                ASSERT_EQ(static_cast<long>(i + i + test_size), res[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(ZipNode, TwoDisbalancedIntegerArrays) {

    // first DIA is heavily balanced to the first workers, second DIA is
    // balanced to the last workers.
    auto start_func =
        [](Context& ctx) {

            // numbers 0..999 (evenly distributed to workers)
            auto input1 = Generate(
                ctx, test_size,
                [](size_t index) { return index; });

            // numbers 1000..1999
            auto input2 = input1.Map(
                [](size_t i) { return static_cast<short>(test_size + i); });

            // numbers 0..99 (concentrated on first workers)
            auto zip_input1 = input1.Filter(
                [](size_t i) { return i < test_size / 10; });

            // numbers 1900..1999 (concentrated on last workers)
            auto zip_input2 = input2.Filter(
                [](size_t i) { return i >= 2 * test_size - test_size / 10; });

            // map to shorts
            auto zip_input2_short = zip_input2.Map(
                [](size_t index) { return static_cast<short>(index); });

            // zip
            auto zip_result = zip_input1.Zip(
                zip_input2_short, [](size_t a, short b) -> MyStruct {
                    return { static_cast<int>(a), b };
                });

            // check result
            std::vector<MyStruct> res = zip_result.Keep().AllGather();

            ASSERT_EQ(test_size / 10, res.size());

            for (size_t i = 0; i != res.size(); ++i) {
                // sLOG1 << i << res[i].a << res[i].b;
                ASSERT_EQ(static_cast<long>(i), res[i].a);
                ASSERT_EQ(static_cast<long>(2 * test_size - test_size / 10 + i), res[i].b);
            }

            // check size of zip (recalculates ZipNode)
            ASSERT_EQ(100u, zip_result.Size());
        };

    api::RunLocalTests(start_func);
}

TEST(ZipNode, TwoDisbalancedIntegerArraysZipWithIndex) {

    // first DIA is heavily balanced to the first workers, second DIA is
    // balanced to the last workers.
    auto start_func =
        [](Context& ctx) {

            // numbers 0..999 (evenly distributed to workers)
            auto input1 = Generate(
                ctx, test_size,
                [](size_t index) { return static_cast<unsigned>(index); });

            // numbers 0..99 (concentrated on first workers)
            auto zip_input1 = input1.Filter(
                [](unsigned i) { return i >= 8 * test_size / 10; });

            // zip
            auto zip_result = zip_input1.ZipWithIndex(
                [](unsigned a, size_t index) -> MyStruct {
                    return { static_cast<int>(a), static_cast<int>(index) };
                });

            // check result
            std::vector<MyStruct> res = zip_result.Keep().AllGather();

            ASSERT_EQ(2 * test_size / 10, res.size());

            for (size_t i = 0; i < res.size(); ++i) {
                // sLOG1 << i << res[i].a << res[i].b;
                ASSERT_EQ(static_cast<int>(8 * test_size / 10 + i), res[i].a);
                ASSERT_EQ(static_cast<int>(i), res[i].b);
            }

            // check size of zip (recalculates ZipNode)
            ASSERT_EQ(200u, zip_result.Size());
        };

    api::RunLocalTests(start_func);
}

TEST(ZipNode, TwoIntegerArraysWhereOneIsEmpty) {

    auto start_func =
        [](Context& ctx) {

            // numbers 0..999 (evenly distributed to workers)
            auto input1 = Generate(
                ctx, test_size,
                [](size_t index) { return index; });

            // numbers 0..999 (evenly distributed to workers)
            auto input2 = Generate(
                ctx, 0,
                [](size_t index) { return index; });

            // map to shorts
            auto input2_short = input2.Map(
                [](size_t index) { return static_cast<short>(index); });

            // zip
            auto zip_result = input1.Zip(
                CutTag, input2_short, [](size_t a, short b) -> long {
                    return static_cast<long>(a) + b;
                });

            // check result
            std::vector<long> res = zip_result.AllGather();
            ASSERT_EQ(0u, res.size());
        };

    api::RunLocalTests(start_func);
}

TEST(ZipNode, TwoDisbalancedStringArrays) {

    // first DIA is heavily balanced to the first workers, second DIA is
    // balanced to the last workers.
    auto start_func =
        [](Context& ctx) {

            // generate random strings with 10..20 characters
            auto input_gen = Generate(
                ctx, test_size,
                [](size_t index) -> std::string {
                    std::default_random_engine rng(
                        123456 + static_cast<unsigned>(index));
                    std::uniform_int_distribution<size_t> length(10, 20);
                    rng(); // skip one number

                    return common::RandomString(
                        length(rng), rng, "abcdefghijklmnopqrstuvwxyz")
                    + std::to_string(index);
                });

            DIA<std::string> input = input_gen.Collapse();

            std::vector<std::string> vinput = input.AllGather();
            ASSERT_EQ(test_size, vinput.size());

            // Filter out strings that start with a-e
            auto input1 = input.Filter(
                [](const std::string& str) { return str[0] <= 'e'; });

            // Filter out strings that start with w-z
            auto input2 = input.Filter(
                [](const std::string& str) { return str[0] >= 'w'; });

            // zip
            auto zip_result = input1.Zip(
                CutTag,
                input2, [](const std::string& a, const std::string& b) {
                    return a + b;
                });

            // check result
            std::vector<std::string> res = zip_result.AllGather();

            // recalculate result locally
            std::vector<std::string> check;
            {
                std::vector<std::string> v1, v2;

                for (size_t index = 0; index < vinput.size(); ++index) {
                    const std::string& s1 = vinput[index];
                    if (s1[0] <= 'e') v1.push_back(s1);
                    if (s1[0] >= 'w') v2.push_back(s1);
                }

                ASSERT_EQ(v1, input1.AllGather());
                ASSERT_EQ(v2, input2.AllGather());

                for (size_t i = 0; i < std::min(v1.size(), v2.size()); ++i) {
                    check.push_back(v1[i] + v2[i]);
                    // sLOG1 << check.back();
                }
            }

            for (size_t i = 0; i != res.size(); ++i) {
                sLOG0 << res[i] << " " << check[i] << (res[i] == check[i]);
            }

            ASSERT_EQ(check.size(), res.size());
            ASSERT_EQ(check, res);
        };

    api::RunLocalTests(start_func);
}

TEST(ZipNode, ThreeIntegerArrays) {

    auto start_func =
        [](Context& ctx) {

            // numbers 0..999 (evenly distributed to workers)
            auto input1 = Generate(
                ctx, test_size,
                [](size_t index) { return static_cast<short>(index); });

            // numbers 0..1999 (evenly distributed to workers)
            auto input2 = Generate(
                ctx, test_size * 2,
                [](size_t index) { return index; });

            // numbers 0..0.999 (evenly distributed to workers)
            auto input3 = Generate(
                ctx, test_size,
                [](size_t index) {
                    return static_cast<double>(index)
                    / static_cast<double>(test_size);
                });

            // zip
            auto zip_result = Zip(
                CutTag,
                [](short a, size_t b, double c) {
                    return std::make_tuple(a, b, c);
                },
                input1, input2, input3);

            // check result
            std::vector<std::tuple<short, size_t, double> > res
                = zip_result.AllGather();

            ASSERT_EQ(test_size, res.size());
            for (size_t i = 0; i < test_size; ++i) {
                ASSERT_EQ(static_cast<short>(i), std::get<0>(res[i]));
                ASSERT_EQ(static_cast<size_t>(i), std::get<1>(res[i]));
                ASSERT_DOUBLE_EQ(
                    static_cast<double>(i) / static_cast<double>(test_size),
                    std::get<2>(res[i]));
            }
        };

    api::RunLocalTests(start_func);
}

TEST(ZipNode, ThreeIntegerArraysPadded) {

    auto start_func =
        [](Context& ctx) {

            // numbers 0..999 (evenly distributed to workers)
            auto input1 = Generate(
                ctx, test_size,
                [](size_t index) { return static_cast<short>(index); });

            // numbers 0..1999 (evenly distributed to workers)
            auto input2 = Generate(
                ctx, test_size * 2,
                [](size_t index) { return index; });

            // numbers 0..0.999 (evenly distributed to workers)
            auto input3 = Generate(
                ctx, test_size,
                [](size_t index) {
                    return static_cast<double>(index)
                    / static_cast<double>(test_size);
                });

            // zip
            auto zip_result = Zip(
                PadTag,
                [](short a, size_t b, double c) {
                    return std::make_tuple(a, b, c);
                },
                std::make_tuple(42, 42, 42),
                input1, input2, input3);

            // check result
            std::vector<std::tuple<short, size_t, double> > res
                = zip_result.AllGather();

            ASSERT_EQ(2 * test_size, res.size());
            for (size_t i = 0; i < 2 * test_size; ++i) {
                ASSERT_EQ(i < test_size ? static_cast<short>(i) : 42,
                          std::get<0>(res[i]));
                ASSERT_EQ(static_cast<size_t>(i),
                          std::get<1>(res[i]));
                ASSERT_DOUBLE_EQ(
                    i < test_size
                    ? static_cast<double>(i) / static_cast<double>(test_size)
                    : 42,
                    std::get<2>(res[i]));
            }
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
