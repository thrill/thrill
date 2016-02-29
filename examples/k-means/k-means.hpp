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

//! A 2-d point with double precision
struct Point2D {
    double x, y;

    double distance(const Point2D& b) const {
        return std::sqrt((x - b.x) * (x - b.x) + (y - b.y) * (y - b.y));
    }
    Point2D operator + (const Point2D& b) const {
        return Point2D { x + b.x, y + b.y };
    }
    Point2D& operator += (const Point2D& b) {
        x += b.x, y += b.y;
        return *this;
    }
    Point2D operator / (double s) const {
        return Point2D { x / s, y / s };
    }
    Point2D& operator /= (double s) {
        x /= s, y /= s;
        return *this;
    }
    friend std::ostream& operator << (std::ostream& os, const Point2D& a) {
        return os << '(' << a.x << ',' << a.y << ')';
    }
};

using Point2DClusterId = std::pair<Point2D, size_t>;

//! A point which contains "count" accumulated vectors.
struct CentroidAccumulated {
    Point2D p;
    size_t  count;
};

//! Assignment of a point to a cluster, which is the input to
struct ClosestCentroid {
    size_t              cluster_id;
    CentroidAccumulated center;
};

//! Calculate k-Means using Lloyd's Algorithm. The DIA centroids is both an
//! input and an output parameter. The method returns a std::pair<Point2D,
//! size_t> = Point2DClusterId into the centroids for each input point.
template <typename InStack>
auto KMeans(const DIA<Point2D, InStack>&input_points, DIA<Point2D>&centroids,
            size_t iterations) {

    auto points = input_points.Cache();

    DIA<ClosestCentroid> closest;

    for (size_t iter = 0; iter < iterations; ++iter) {

        // handling this local variable is difficult: it is calculated as an
        // Action here, but must exist later when the Map() is
        // processed. Hhence, we move it into the closure.
        std::vector<Point2D> local_centroids = centroids.AllGather();
        size_t num_centroids = local_centroids.size();

        // calculate the closest centroid for each point
        closest = points.Map(
            [local_centroids = std::move(local_centroids)](const Point2D& p) {
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
            });

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
                           return Point2DClusterId(cc.center.p, cc.cluster_id);
                       });
}

} // namespace k_means
} // namespace examples

#endif // !THRILL_EXAMPLES_K_MEANS_K_MEANS_HEADER

/******************************************************************************/
