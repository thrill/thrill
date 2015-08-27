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

static const bool debug = true;

TEST(GroupByNode, Compile_and_Sum) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            int n = 16;
            int m = 4;
            int result_sum = (n * n + n )/2;
            LOG << "RUNNING NEW TEST WITH EXPECTED SUM " << result_sum;

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return index + 1;
                },
                n);

            auto keyfn = [](size_t in) { return (in % 4); };

            auto add_fn =
            [](data::File::Reader r) {
                    int res = 0;
                    int k = 0;
                    while(r.HasNext()) {
                        auto n = r.template Next<int>();
                        k = n % 4;
                        res += n;
                    }
                    // LOG << "SUM OF KEY " << k << " IS " << res;
                    return res;
                };

            auto reduced = integers.GroupBy(keyfn, add_fn);

            auto add_function = [](int in1, int in2) {
                                    return in1 + in2;
                                };

            ASSERT_EQ(result_sum, reduced.Sum(add_function));

            std::vector<int> out_vec = reduced.AllGather();
            std::sort(out_vec.begin(), out_vec.end());

            // ???
            std::vector<int> resultref(m, 0);
            for (int t = 0; t <= n; ++t) {
                resultref[t%m] += t;
            }

            LOG << "   OUT VEC IS";
            for (std::size_t i = 0; i < out_vec.size(); ++i) {
                // ASSERT_EQ(out_vec[i], resultref[i]);
                LOG << "      ELEMENT IS " << out_vec[i];
            }

            // ASSERT_EQ((size_t)2, out_vec.size());
        };

    api::RunLocalTests(start_func);
}


/******************************************************************************/
