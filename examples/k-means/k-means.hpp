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

#include <thrill/api/all_gather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/reduce_to_index.hpp>

#include <cereal/types/vector.hpp>
#include <thrill/data/serialization_cereal.hpp>

#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace examples {
namespace k_means {

using thrill::DIA;

/******************************************************************************/
// Compile-Time Fixed-Dimensional Points

//! A D-dimensional point with double precision
template <size_t D>
struct Point {
    double        x[D];

    static size_t dim() { return D; }

    static Point  Origin() {
        Point p;
        std::fill(p.x, p.x + D, 0.0);
        return p;
    }
    template <typename Distribution, typename Generator>
    static Point Random(size_t dim, Distribution& dist, Generator& gen) {
        assert(dim == D);
        Point p;
        for (size_t i = 0; i < D; ++i) p.x[i] = dist(gen);
        return p;
    }
    double        distance(const Point& b) const {
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

/******************************************************************************/
// Variable-Dimensional Points

//! A D-dimensional point with double precision
struct VPoint {

    using Vector = std::vector<double>;
    Vector        v_;

    explicit VPoint(size_t D = 0) : v_(D) { }
    explicit VPoint(Vector&& v) : v_(std::move(v)) { }

    size_t        dim() const { return v_.size(); }

    static VPoint Origin(size_t D) {
        VPoint p(D);
        std::fill(p.v_.begin(), p.v_.end(), 0.0);
        return p;
    }
    template <typename Distribution, typename Generator>
    static VPoint Random(size_t D, Distribution& dist, Generator& gen) {
        VPoint p(D);
        for (size_t i = 0; i < D; ++i) p.v_[i] = dist(gen);
        return p;
    }
    double        distance(const VPoint& b) const {
        assert(v_.size() == b.v_.size());
        double sum = 0.0;
        for (size_t i = 0; i < v_.size(); ++i)
            sum += (v_[i] - b.v_[i]) * (v_[i] - b.v_[i]);
        return std::sqrt(sum);
    }
    VPoint operator + (const VPoint& b) const {
        assert(v_.size() == b.v_.size());
        VPoint p(v_.size());
        for (size_t i = 0; i < v_.size(); ++i) p.v_[i] = v_[i] + b.v_[i];
        return p;
    }
    VPoint& operator += (const VPoint& b) {
        assert(v_.size() == b.v_.size());
        for (size_t i = 0; i < v_.size(); ++i) v_[i] += b.v_[i];
        return *this;
    }
    VPoint operator / (double s) const {
        VPoint p(v_.size());
        for (size_t i = 0; i < v_.size(); ++i) p.v_[i] = v_[i] / s;
        return p;
    }
    VPoint& operator /= (double s) {
        for (size_t i = 0; i < v_.size(); ++i) v_[i] /= s;
        return *this;
    }
    friend std::ostream& operator << (std::ostream& os, const VPoint& a) {
        os << '(' << a.v_[0];
        for (size_t i = 1; i != a.v_.size(); ++i) os << ',' << a.v_[i];
        return os << ')';
    }

    template <typename Archive>
    void serialize(Archive& archive) {
        archive(v_);
    }
};

/******************************************************************************/

template <typename Point>
using PointClusterId = std::pair<Point, size_t>;

//! A point which contains "count" accumulated vectors.
template <typename Point>
struct CentroidAccumulated {
    Point  p;
    size_t count;

    template <typename Archive>
    void serialize(Archive& archive) {
        archive(p, count);
    }
};

//! Assignment of a point to a cluster, which is the input to
template <typename Point>
struct ClosestCentroid {
    size_t                     cluster_id;
    CentroidAccumulated<Point> center;

    template <typename Archive>
    void serialize(Archive& archive) {
        archive(cluster_id, center);
    }
};

//! Calculate k-Means using Lloyd's Algorithm. The DIA centroids is both an
//! input and an output parameter. The method returns a std::pair<Point2D,
//! size_t> = Point2DClusterId into the centroids for each input point.
template <typename Point, typename InStack>
auto KMeans(const DIA<Point, InStack>&input_points, DIA<Point>&centroids,
            size_t iterations) {

    auto points = input_points.Cache();

    using ClosestCentroid = ClosestCentroid<Point>;
    using CentroidAccumulated = CentroidAccumulated<Point>;

    DIA<ClosestCentroid> closest;

    for (size_t iter = 0; iter < iterations; ++iter) {

        // handling this local variable is difficult: it is calculated as an
        // Action here, but must exist later when the Map() is
        // processed. Hhence, we move it into the closure.
        std::vector<Point> local_centroids = centroids.AllGather();
        size_t num_centroids = local_centroids.size();

        // calculate the closest centroid for each point
        closest = points.Map(
            [local_centroids = std::move(local_centroids)](const Point& p) {
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
                     return cc.center.p / static_cast<double>(cc.center.count);
                 })
            .Collapse();
    }

    // map to only the index.
    return closest.Map(
        [](const ClosestCentroid& cc) {
            return PointClusterId<Point>(cc.center.p, cc.cluster_id);
        });
}

} // namespace k_means
} // namespace examples

#endif // !THRILL_EXAMPLES_K_MEANS_K_MEANS_HEADER

/******************************************************************************/
