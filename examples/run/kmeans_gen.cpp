/*******************************************************************************
 * examples/run/kmeans_gen.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/kmeans.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <vector>

using thrill::DIA;
using thrill::Context;

using namespace thrill; // NOLINT

using Centrioid = std::pair<float, float>;

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    int n;
    clp.AddParamInt("n", n, "number of points");

    int k;
    clp.AddParamInt("k", k, "Number of clusters");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    std::function<void(Context&)> start_func =
        [&n, &k](api::Context& ctx) {
            ctx.enable_consume(false);

            std::default_random_engine rng(std::random_device { } ());
            std::uniform_real_distribution<float> dist(0.0, 100000.0);

            auto points = Generate(
                ctx,
                [&](const size_t& index) {
                    (void)index;
                    return std::to_string(dist(rng)) + " " + std::to_string(dist(rng));
                },
                n);

            auto centroids = Generate(
                ctx,
                [&](const size_t& index) {
                    (void)index;
                    return std::to_string(dist(rng)) + " " + std::to_string(dist(rng));
                },
                k);

            auto clusters = examples::kMeans(points, centroids);

            std::vector<Centrioid> cs = clusters.AllGather();

            if (!ctx.my_rank()) {
                for (Centrioid c : cs) {
                    LOG1 << "centroid x: " << c.first << " y: " << c.second;
                }
            }
        };

    return api::Run(start_func);
}

/******************************************************************************/
