/*******************************************************************************
 * examples/k-means/k-means_run.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/k-means/k-means.hpp>

#include <thrill/api/gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;            // NOLINT
using namespace examples::k_means; // NOLINT

using Point2D = Point<2>;

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    bool generate = false;
    clp.AddFlag('g', "generate", generate,
                "generate random data, set input = #points");

    size_t iter = 10;
    clp.AddSizeT('n', "iterations", iter, "PageRank iterations, default: 10");

    int num_points;
    clp.AddParamInt("points", num_points, "number of points");

    int num_clusters;
    clp.AddParamInt("clusters", num_clusters, "Number of clusters");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    auto start_func =
        [&](api::Context& ctx) {
            ctx.enable_consume();

            std::default_random_engine rng(std::random_device { } ());
            std::uniform_real_distribution<float> dist(0.0, 100000.0);

            auto points = Generate(
                ctx, [&](const size_t& /* index */) {
                    return Point2D { { dist(rng), dist(rng) } };
                }, num_points);

            DIA<Point2D> centroids_dia = Generate(
                ctx, [&](const size_t& /* index */) {
                    return Point2D { { dist(rng), dist(rng) } };
                }, num_clusters);

            auto result = KMeans(points, centroids_dia, iter);

            std::vector<PointClusterId<2> > plist = result.Gather();
            std::vector<Point2D> centroids = centroids_dia.Gather();

            if (ctx.my_rank() == 0) {
                for (const PointClusterId<2> & p : plist) {
                    LOG1 << p.first << " -> " << p.second;
                }
            }
        };

    return api::Run(start_func);
}

/******************************************************************************/
