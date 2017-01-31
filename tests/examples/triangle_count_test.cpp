/*******************************************************************************
 * tests/examples/triangle_count_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/triangles/triangles.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/generate.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <limits>
#include <utility>
#include <vector>

using namespace thrill;
using namespace examples::triangles;

TEST(TriangleCount, FullyConnected) {

    auto start_func =
        [&](Context& ctx) {
            size_t size = 100;

            auto input = Generate(
                ctx, size);

            auto edges = input.template FlatMap<Edge>(
                [&size](const size_t& index, auto emit) {
                    for (size_t target = index + 1; target < size; ++target) {
                        emit(std::make_pair(index, target));
                    }
                }).Cache();

            size_t size_over_3 = size * (size - 1) * (size - 2) / 6;

            ASSERT_EQ(CountTriangles(edges), size_over_3);
        };

    api::RunLocalTests(start_func);
}

TEST(TriangleCount, FullyConnectedWithMultiEdges) {

    auto start_func =
        [&](Context& ctx) {
            size_t size = 100;

            auto input = Generate(
                ctx, size);

            auto edges = input.template FlatMap<Edge>(
                [&size](const size_t& index, auto emit) {
                    for (size_t target = index + 1; target < size; ++target) {
                        emit(std::make_pair(index, target));
                        emit(std::make_pair(index, target));
                    }
                }).Cache();

            size_t size_over_3 = size * (size - 1) * (size - 2) / 6;

            ASSERT_EQ(CountTriangles(edges), size_over_3 * 8);
        };

    api::RunLocalTests(start_func);
}

TEST(TriangleCount, SomewhatSparse) {

    auto start_func =
        [&](Context& ctx) {
            size_t size = 1000;
            size_t multiple = 10;

            auto input = Generate(
                ctx, size);

            auto edges = input.template FlatMap<Edge>(
                [&size, &multiple](const size_t& index, auto emit) {
                    for (size_t target = index + multiple; target < size; target = target + multiple) {
                        emit(std::make_pair(index, target));
                    }
                }).Cache();

            size_t size_over_3 = multiple * (size / multiple) * ((size / multiple) - 1) * ((size / multiple) - 2) / 6;

            ASSERT_EQ(CountTriangles(edges), size_over_3);
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
