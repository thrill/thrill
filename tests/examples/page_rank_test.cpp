/*******************************************************************************
 * tests/examples/page_rank_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/page_rank/page_rank.hpp>
#include <examples/page_rank/zipf_graph_gen.hpp>

#include <thrill/api/allgather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/equal_to_dia.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;
using namespace examples::page_rank;

/******************************************************************************/
// Zipf generated graph

TEST(PageRank, RandomZipfGraph) {
    static constexpr bool debug = false;

    static constexpr size_t iterations = 5;
    static constexpr size_t num_pages = 1000;
    static constexpr double dampening = 0.85;

    // calculate correct result
    std::vector<double> correct_page_rank;

    // generated outgoing links graph
    std::vector<OutgoingLinks> outlinks(num_pages);

    {
        ZipfGraphGen graph_gen(num_pages);
        std::minstd_rand rng(123456);
        for (size_t i = 0; i < num_pages; ++i) {
            outlinks[i] = graph_gen.GenerateOutgoing(rng);
        }

        // initial ranks: 1 / n
        std::vector<double> ranks(num_pages, 1.0 / num_pages);

        // contribution of rank weight in each iteration
        std::vector<double> contrib(num_pages, 0.0);

        for (size_t iter = 0; iter < iterations; ++iter) {
            // iterate over pages, send weight to targets
            for (size_t p = 0; p < num_pages; ++p) {
                OutgoingLinks& links = outlinks[p];
                for (size_t t = 0; t < links.size(); ++t) {
                    contrib[links[t]] += ranks[p] / links.size();
                }
            }
            // calculate new ranks from contributions
            for (size_t p = 0; p < num_pages; ++p) {
                ranks[p] = dampening * contrib[p] + (1 - dampening) / num_pages;
                contrib[p] = 0.0;
            }
        }

        for (size_t p = 0; p < num_pages; ++p) {
            LOG << "pr[" << p << "] = " << ranks[p];
        }

        correct_page_rank = ranks;
    }

    auto start_func =
        [&outlinks, &correct_page_rank](Context& ctx) {
            ctx.enable_consume();

            auto links = EqualToDIA(ctx, outlinks).Cache();

            auto page_rank = PageRank(links, num_pages, iterations);

            // compare results
            std::vector<double> result = page_rank.AllGather();

            ASSERT_EQ(correct_page_rank.size(), result.size());
            for (size_t i = 0; i < result.size(); ++i) {
                ASSERT_TRUE(std::abs(correct_page_rank[i] - result[i]) < 0.000001);
            }
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
