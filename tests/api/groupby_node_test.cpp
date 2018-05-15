/*******************************************************************************
 * tests/api/groupby_node_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 * Copyright (C) 2017 Tim Zeitz <dev.tim.zeitz@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/all_gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/group_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/common/logger.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

static constexpr bool debug = false;

TEST(GroupByNode, CompileAndSum) {

    auto start_func =
        [](Context& ctx) {
            size_t n = 8;
            static constexpr size_t m = 4;

            auto sizets = Generate(ctx, n);

            auto modulo_keyfn = [](size_t in) { return (in % m); };

            auto sum_fn =
                [](auto& r, size_t /* key */) {
                    size_t res = 0;
                    while (r.HasNext()) {
                        size_t n = r.Next();
                        res += n;
                    }
                    return res;
                };

            // group by to compute sum and gather results
            auto reduced = sizets.GroupByKey<size_t>(modulo_keyfn, sum_fn);
            std::vector<size_t> out_vec = reduced.AllGather();

            // compute vector with expected results
            std::vector<size_t> res_vec(m, 0);
            for (size_t t = 0; t < n; ++t) {
                res_vec[t % m] += t;
            }

            std::sort(out_vec.begin(), out_vec.end());
            std::sort(res_vec.begin(), res_vec.end());

            LOG << "res_vec " << "/" << " out_vec";
            for (size_t i = 0; i < res_vec.size(); ++i) {
                LOG << res_vec[i] << " / " << out_vec[i];
                ASSERT_EQ(res_vec[i], out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(GroupByNode, Median) {

    auto start_func =
        [](Context& ctx) {
            size_t n = 9999;
            static constexpr size_t m = 4;

            auto sizets = Generate(ctx, n);

            auto modulo_keyfn = [](size_t in) { return (in % m); };

            auto median_fn =
                [](auto& r, size_t /* key */) {
                    std::vector<size_t> all;
                    while (r.HasNext()) {
                        all.push_back(r.Next());
                    }
                    std::sort(std::begin(all), std::end(all));
                    for (auto c : all) {
                        LOG << c;
                    }
                    return all[all.size() / 2 - 1];
                };

            // group by to compute sum and gather results
            auto reduced = sizets.GroupByKey<size_t>(modulo_keyfn, median_fn);
            std::vector<size_t> out_vec = reduced.AllGather();

            // compute vector with expected results
            std::vector<std::vector<size_t> > res_vecvec(m);
            std::vector<size_t> res_vec(m, 0);
            for (size_t t = 1; t < n; ++t) {
                res_vecvec[t % m].push_back(t);
            }
            for (size_t i = 0; i < res_vecvec.size(); ++i) {
                std::sort(std::begin(res_vecvec[i]), std::end(res_vecvec[i]));
                res_vec[i] = res_vecvec[i][res_vecvec[i].size() / 2 - 1];
            }

            std::sort(out_vec.begin(), out_vec.end());
            std::sort(res_vec.begin(), res_vec.end());

            LOG << "res_vec " << "/" << " out_vec";
            for (size_t i = 0; i < res_vec.size(); ++i) {
                LOG << res_vec[i] << " / " << out_vec[i];
                ASSERT_EQ(res_vec[i], out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(GroupByNode, GroupToIndexCorrectResults) {

    auto start_func =
        [](Context& ctx) {
            size_t n = 9999;
            static constexpr size_t m = 31;

            auto integers = Generate(ctx, n);

            auto key = [](size_t in) {
                           return in % m;
                       };

            auto add_function =
                [](auto& r, size_t /* key */) {
                    size_t res = 42;
                    while (r.HasNext()) {
                        res += r.Next();
                    }
                    return res;
                };

            auto reduced = integers.GroupToIndex<size_t>(key, add_function, m);

            std::vector<size_t> out_vec = reduced.AllGather();

            // compute vector with expected results
            std::vector<size_t> res_vec(m, 42);
            for (size_t t = 0; t < n; ++t) {
                res_vec[t % m] += t;
            }

            LOG << "res_vec " << "/" << " out_vec";
            for (size_t i = 0; i < res_vec.size(); ++i) {
                LOG << res_vec[i] << " / " << out_vec[i];
                ASSERT_EQ(res_vec[i], out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(GroupByNode, GroupToIndexCorrectSize) {

    auto start_func =
        [](Context& ctx) {
            static constexpr size_t buckets = 10;

            auto integers = Generate(ctx, 500);

            auto key = [](size_t x) {
                           return x % (buckets / 2);
                       };

            auto add_function =
                [](auto& r, size_t /* key */) {
                    size_t res = 42;
                    while (r.HasNext()) {
                        res += r.Next();
                    }
                    return res;
                };

            size_t result_size = integers.GroupToIndex<size_t>(key, add_function, buckets).Size();

            ASSERT_EQ(buckets, result_size);
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
