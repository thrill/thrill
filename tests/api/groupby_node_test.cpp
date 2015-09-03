/*******************************************************************************
 * tests/api/groupby_node_test.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/


#include <thrill/api/allgather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/groupby.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>


#include <thrill/core/stxxl_multiway_merge.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>
#include <cstdlib>

using namespace thrill; // NOLINT

static const bool debug = false;

TEST(GroupByNode, Compile_and_Sum) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            int n = 100000;
            int m = 10;

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return index + 1;
                },
                n);

            auto modulo_keyfn = [m](size_t in) { return (in % m); };

            auto sum_fn =
            [m](data::File::Reader r) {
                    int res = 0;
                    int k = 0;
                    while(r.HasNext()) {
                        auto n = r.template Next<int>();
                        k = n % m;
                        res += n;
                    }
                    return res;
                };

            // group by to compute sum and gather results
            auto reduced = integers.GroupBy(modulo_keyfn, sum_fn);
            std::vector<int> out_vec = reduced.AllGather();

            // compute vector with expected results
            std::vector<int> res_vec(m, 0);
            for (int t = 0; t <= n; ++t) {
                res_vec[t%m] += t;
            }

            std::sort(out_vec.begin(), out_vec.end());
            std::sort(res_vec.begin(), res_vec.end());

            LOG << "out_vec " << "/" << " res_vec";
            for (std::size_t i = 0; i < out_vec.size(); ++i) {
                ASSERT_EQ(out_vec[i], res_vec[i]);
                LOG << out_vec[i] << " / " << res_vec[i];
            }

        };

    // api::RunLocalTests(start_func);
        api::RunLocalMock(2, 1, start_func);
}


/******************************************************************************/
