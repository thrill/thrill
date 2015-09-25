/*******************************************************************************
 * tests/api/graph_test.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/api/allgather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/context.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/generate_from_file.hpp>
#include <thrill/api/prefixsum.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/stats_graph.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/logger.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <random>
#include <string>
#include <vector>

using namespace thrill;
using api::StatsGraph;
using api::StatsNode;

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

            DIA<size_t> squares = integers.Collapse();
            size_t sum = 0;

            // run loop four times, inflating DIA of 16 items -> 256
            while (sum < 64) {
                auto pairs = squares.FlatMap(flatmap_duplicate);
                auto multiplied = pairs.Map(map_multiply);
                squares = multiplied.Cache();
                sum = squares.Size();
            }

            std::vector<size_t> out_vec = squares.AllGather();

            ASSERT_EQ(64u, out_vec.size());
            ASSERT_EQ(64u, squares.Size());

            ctx.stats_graph().BuildLayout("loop.out");
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
