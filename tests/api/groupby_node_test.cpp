/*******************************************************************************
 * tests/api/groupby_node_test.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/api/allgather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/groupby.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>

#include <gtest/gtest.h>
#include <thrill/core/stxxl_multiway_merge.hpp>

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

static const bool debug = false;

TEST(GroupByNode, Compile_and_Sum) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            unsigned n = 99999;
            unsigned m = 4;

            auto sizets = Generate(
                ctx,
                [](const size_t& index) {
                    return static_cast<std::size_t>(index + 1);
                },
                n);

            auto modulo_keyfn = [m](size_t in) { return (in % m); };

            auto sum_fn =
                [m](api::GroupByIterator<std::size_t, decltype(modulo_keyfn)>& r) {
                    auto res = 0;
                    int k = 0;
                    while (r.HasNext()) {
                        auto n = r.Next();
                        k = n % m;
                        res += n;
                    }
                    return static_cast<int>(res);
                };

            // group by to compute sum and gather results
            auto reduced = sizets.GroupBy(modulo_keyfn, sum_fn);
            std::vector<int> out_vec = reduced.AllGather();

            // compute vector with expected results
            std::vector<unsigned> res_vec(m, 0);
            for (unsigned t = 0; t <= n; ++t) {
                res_vec[t % m] += t;
            }

            std::sort(out_vec.begin(), out_vec.end());
            std::sort(res_vec.begin(), res_vec.end());

            LOG << "res_vec " << "/" << " out_vec";
            for (std::size_t i = 0; i < res_vec.size(); ++i) {
                LOG << res_vec[i] << " / " << out_vec[i];
                ASSERT_EQ(res_vec[i], out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}


TEST(GroupByNode, Median) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            unsigned n = 99999;
            unsigned m = 4;

            auto sizets = Generate(
                ctx,
                [](const size_t& index) {
                    return index + 1;
                },
                n);

            auto modulo_keyfn = [m](size_t in) { return (in % m); };

            auto median_fn =
                [m](api::GroupByIterator<std::size_t, decltype(modulo_keyfn)>& r) {
                    std::vector<std::size_t> all;
                    while (r.HasNext()) {
                        all.push_back(r.Next());
                    }
                    std::sort(std::begin(all), std::end(all));
                    for (auto c : all) {
                        LOG << c;
                    }
                    return static_cast<int>(all[all.size()/2 - 1]);
                };

            // group by to compute sum and gather results
            auto reduced = sizets.GroupBy(modulo_keyfn, median_fn);
            std::vector<int> out_vec = reduced.AllGather();

            // compute vector with expected results
            std::vector<std::vector<unsigned>> res_vecvec(m);
            std::vector<unsigned> res_vec(m, 0);
            for (unsigned t = 1; t <= n; ++t) {
                res_vecvec[t % m].push_back(t);
            }
            for (std::size_t i = 0; i < res_vecvec.size(); ++i) {
                std::sort(std::begin(res_vecvec[i]), std::end(res_vecvec[i]));
                res_vec[i] = res_vecvec[i][res_vecvec[i].size()/2 - 1];
            }

            std::sort(out_vec.begin(), out_vec.end());
            std::sort(res_vec.begin(), res_vec.end());

            LOG << "res_vec " << "/" << " out_vec";
            for (std::size_t i = 0; i < res_vec.size(); ++i) {
                LOG << res_vec[i] << " / " << out_vec[i];
                ASSERT_EQ(res_vec[i], out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
