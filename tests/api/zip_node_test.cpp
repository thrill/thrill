/*******************************************************************************
 * tests/api/zip_node_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/bootstrap.hpp>
#include <c7a/api/generate.hpp>
#include <c7a/api/zip.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <string>

using namespace c7a;

using c7a::api::Context;
using c7a::api::DIARef;

TEST(ZipNode, GenerateTwoIntegerArraysAndZipThem) {

    static const size_t test_size = 1000;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // numbers 0..999 (evenly distributed to workers)
            auto input1 = Generate(
                ctx,
                [](size_t index) { return index; },
                test_size);

            // numbers 1000..1999
            auto input2 = input1.Map(
                [](size_t i) { return static_cast<short>(test_size + i); });

            // numbers 0..99 (concentrated on first workers)
            auto zip_input1 = input1.Filter(
                [](size_t i) { return i < test_size / 10; });

            // numbers 1900..1999 (concentrated on last workers)
            auto zip_input2 = input2.Filter(
                [](size_t i) { return i >= 2 * test_size - test_size / 10; });

            auto zip_result = input1.Zip(
                [](size_t a, short b) { return a + b; }, input2);
        };

    c7a::api::ExecuteLocalTests(start_func);
}

/******************************************************************************/
