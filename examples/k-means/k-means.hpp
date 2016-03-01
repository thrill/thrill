/*******************************************************************************
 * examples/k-means/k-means.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_K_MEANS_K_MEANS_HEADER
#define THRILL_EXAMPLES_K_MEANS_K_MEANS_HEADER

#include <thrill/api/allgather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/reduce_to_index.hpp>

#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace examples {
namespace k_means {

using namespace thrill; // NOLINT

//! A D-dimensional point with double precision
template <size_t D>
struct Point {
    double       x[D];

    static Point Origin() {
        Point p;
        std::fill(p.x, p.x + D, 0.0);
        return p;
    }
    double       distance(const Point& b) const {
        double sum = 0.0;
        for (size_t i = 0; i < D; ++i) sum += (x[i] - b.x[i]) * (x[i] - b.x[i]);
        return std::sqrt(sum);
    }
    Point operator + (const Point& b) const {
        Point p;
        for (size_t i = 0; i < D; ++i) p.x[i] = x[i] + b.x[i];
        return p;
    }
    Point& operator += (const Point& b) {
        for (size_t i = 0; i < D; ++i) x[i] += b.x[i];
        return *this;
    }
    Point operator / (double s) const {
        Point p;
        for (size_t i = 0; i < D; ++i) p.x[i] = x[i] / s;
        return p;
    }
    Point& operator /= (double s) {
        for (size_t i = 0; i < D; ++i) x[i] /= s;
        return *this;
    }
    friend std::ostream& operator << (std::ostream& os, const Point& a) {
        os << '(' << a.x[0];
        for (size_t i = 1; i != D; ++i) os << ',' << a.x[i];
        return os << ')';
    }
};

template <size_t D>
using PointClusterId = std::pair<Point<D>, size_t>;

//! A point which contains "count" accumulated vectors.
template <size_t D>
struct CentroidAccumulated {
    Point<D> p;
    size_t   count;
};

//! Assignment of a point to a cluster, which is the input to
template <size_t D>
struct ClosestCentroid {
    size_t                 cluster_id;
    CentroidAccumulated<D> center;
};

//! Calculate k-Means using Lloyd's Algorithm. The DIA centroids is both an
//! input and an output parameter. The method returns a std::pair<Point2D,
//! size_t> = Point2DClusterId into the centroids for each input point.
template <size_t D, typename InStack>
auto KMeans(const DIA<Point<D>, InStack>&input_points, DIA<Point<D> >&centroids,
            size_t iterations) {

    auto points = input_points.Cache();

    using ClosestCentroid = ClosestCentroid<D>;
    using CentroidAccumulated = CentroidAccumulated<D>;

    DIA<ClosestCentroid> closest;

    for (size_t iter = 0; iter < iterations; ++iter) {

        // handling this local variable is difficult: it is calculated as an
        // Action here, but must exist later when the Map() is
        // processed. Hhence, we move it into the closure.
        std::vector<Point<D> > local_centroids = centroids.AllGather();
        size_t num_centroids = local_centroids.size();

        // calculate the closest centroid for each point
        closest = points.Map(
            [local_centroids = std::move(local_centroids)](const Point<D>& p) {
                assert(local_centroids.size());
                double min_dist = p.distance(local_centroids[0]);
                size_t closest_id = 0;

                for (size_t i = 1; i < local_centroids.size(); ++i) {
                    double dist = p.distance(local_centroids[i]);
                    if (dist < min_dist) {
                        min_dist = dist;
                        closest_id = i;
                    }
                }
                return ClosestCentroid {
                    closest_id, CentroidAccumulated { p, 1 }
                };
            }).Collapse();

        // Calculate new centroids as the mean of all points associated with
        // it. We use ReduceToIndex only because then the indices in the
        // "closest" stay valid.
        centroids =
            closest
            .ReduceToIndex(
                [](const ClosestCentroid& cc) { return cc.cluster_id; },
                [](const ClosestCentroid& a, const ClosestCentroid& b) {
                    return ClosestCentroid {
                        a.cluster_id,
                        CentroidAccumulated { a.center.p + b.center.p,
                                              a.center.count + b.center.count }
                    };
                },
                num_centroids)
            .Map([](const ClosestCentroid& cc) {
                     return cc.center.p / cc.center.count;
                 })
            .Collapse();
    }

    // map to only the index.
    return closest.Map([](const ClosestCentroid& cc) {
                           return PointClusterId<D>(cc.center.p, cc.cluster_id);
                       });
}

} // namespace k_means
} // namespace examples

#endif // !THRILL_EXAMPLES_K_MEANS_K_MEANS_HEADER

/******************************************************************************/
