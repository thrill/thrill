/*******************************************************************************
 * tests/api/graph_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/allgather.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/api/generate.hpp>
#include <c7a/api/generate_from_file.hpp>
#include <c7a/api/collapse.hpp>
#include <c7a/api/cache.hpp>
#include <c7a/api/prefixsum.hpp>
#include <c7a/api/read_lines.hpp>
#include <c7a/api/size.hpp>
#include <c7a/api/stats_graph.hpp>
#include <c7a/api/write.hpp>
#include <c7a/api/zip.hpp>
#include <c7a/common/logger.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <random>
#include <string>
#include <vector>

using namespace c7a;
using c7a::api::StatsGraph;
using c7a::api::StatsNode;
using c7a::api::Context;
using c7a::api::DIARef;

TEST(Graph, SimpleGraph) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = ReadLines(ctx, "test1")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });

            auto doubled = integers.Map([](int input) { return input * 2; });
            auto filtered = doubled.Filter([](int input) { return input % 2; });
            auto emitted = filtered.FlatMap([](int input, auto emit) { emit(input); });
            auto prefixsums = filtered.PrefixSum();

            auto zip_result = prefixsums.Zip(emitted, [](int input1, int input2) {
                                                 return input1 + input2;
                                             });

            ctx.stats_graph().BuildLayout("simple.out");
        };

    api::RunLocalTests(start_func);
}

TEST(Graph, WhileLoop) {

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
            while (sum < 64) {
                auto pairs = squares.FlatMap(flatmap_duplicate);
                auto multiplied = pairs.Map(map_multiply);
                squares = multiplied.Cache();
                sum = squares.Size();
            }

            std::vector<int> out_vec = squares.AllGather();

            ASSERT_EQ(64u, out_vec.size());
            ASSERT_EQ(64u, squares.Size());

            ctx.stats_graph().BuildLayout("loop.out");
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
