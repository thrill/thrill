/*******************************************************************************
 * tests/examples/k_means_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/k-means/k-means.hpp>

#include <thrill/api/all_gather.hpp>
#include <thrill/api/equal_to_dia.hpp>

#include <tlx/math/abs_diff.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <limits>
#include <utility>
#include <vector>

using namespace thrill;
using namespace examples::k_means;

using Point2D = Point<2>;

TEST(KMeans, RandomPoints) {

    static constexpr size_t iterations = 4;
    static constexpr size_t num_points = 1000;
    static constexpr size_t num_clusters = 20;

    // generate some random points and centroids
    std::vector<Point2D> points, centroids;

    std::default_random_engine rng(123456);
    std::uniform_real_distribution<float> coord_dist(0.0, 100000.0);

    points.reserve(num_points);
    for (size_t i = 0; i < num_points; ++i) {
        points.emplace_back(Point2D {
                                { coord_dist(rng), coord_dist(rng) }
                            });
    }

    centroids.reserve(num_clusters);
    for (size_t i = 0; i < num_clusters; ++i) {
        centroids.emplace_back(points[rng() % points.size()]);
    }

    const std::vector<Point2D> orig_centroids = centroids;

    double correct_cost = 0.0;

    // calculate "correct" results with Lloyd's Algorithm
    {
        std::vector<size_t> closest(num_points);
        std::vector<size_t> point_count(num_clusters);

        for (size_t iter = 0; iter < iterations; ++iter) {

            // for each point, find the closest centroid
            for (size_t i = 0; i < num_points; ++i) {
                const Point2D& p = points[i];
                double min_dist = std::numeric_limits<double>::max();

                for (size_t c = 0; c < num_clusters; ++c) {
                    double dist = p.DistanceSquare(centroids[c]);
                    if (dist < min_dist) {
                        min_dist = dist;
                        closest[i] = c;
                    }
                }
            }

            // calculate new centroids from associated points
            std::fill(point_count.begin(), point_count.end(), 0);
            std::fill(centroids.begin(), centroids.end(), Point2D::Origin());

            for (size_t i = 0; i < num_points; ++i) {
                centroids[closest[i]] += points[i];
                point_count[closest[i]]++;
            }
            for (size_t c = 0; c < num_clusters; ++c) {
                centroids[c] /= static_cast<double>(point_count[c]);
            }
        }

        // calculate distance squared for each point to the closest centroid
        for (size_t i = 0; i < num_points; ++i) {
            const Point2D& p = points[i];
            double min_dist = p.DistanceSquare(centroids[0]);

            for (size_t c = 1; c < num_clusters; ++c) {
                double dist = p.DistanceSquare(centroids[c]);
                if (dist < min_dist) {
                    min_dist = dist;
                }
            }
            correct_cost += min_dist;
        }
    }

    auto start_func =
        [&](Context& ctx) {
            ctx.enable_consume();

            auto input_points = EqualToDIA(ctx, points);
            DIA<Point2D> centroids_dia = EqualToDIA(ctx, orig_centroids);

            auto means = KMeans(input_points.Keep(), 2, num_clusters, iterations);

            double cost = means.ComputeCost(input_points);
            if (ctx.my_rank() == 0) {
                sLOG1 << "cost" << cost << "correct_cost" << correct_cost
                      << "abs_diff_percent"
                      << tlx::abs_diff(cost, correct_cost) / correct_cost;
            }

            ASSERT_LE(tlx::abs_diff(cost, correct_cost) / correct_cost, 0.4);
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
