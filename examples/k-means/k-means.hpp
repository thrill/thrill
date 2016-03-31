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
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/sample.hpp>
#include <thrill/api/sum.hpp>

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
    double        DistanceSquare(const Point& b) const {
        double sum = 0.0;
        for (size_t i = 0; i < D; ++i) sum += (x[i] - b.x[i]) * (x[i] - b.x[i]);
        return sum;
    }
    double        Distance(const Point& b) const {
        return std::sqrt(DistanceSquare(b));
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
    double        DistanceSquare(const VPoint& b) const {
        assert(v_.size() == b.v_.size());
        double sum = 0.0;
        for (size_t i = 0; i < v_.size(); ++i)
            sum += (v_[i] - b.v_[i]) * (v_[i] - b.v_[i]);
        return sum;
    }
    double        Distance(const VPoint& b) const {
        return std::sqrt(DistanceSquare(b));
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

//! Model returned by KMeans algorithm containing results.
template <typename Point>
class KMeansModel
{
public:
    KMeansModel(size_t dimensions, size_t num_clusters, size_t iterations,
                const std::vector<Point>& centroids)
        : dimensions_(dimensions), num_clusters_(num_clusters),
          iterations_(iterations), centroids_(centroids)
    { }

    //! \name Accessors
    //! \{

    //! Returns dimensions_
    size_t dimensions() const { return dimensions_; }

    //! Returns number of clusters
    size_t num_clusters() const { return num_clusters_; }

    //! Returns iterations_
    size_t iterations() const { return iterations_; }

    //! Returns centroids_
    const std::vector<Point>& centroids() const { return centroids_; }

    //! \}

    //! \name Classification
    //! \{

    //! Calculate closest cluster to point
    size_t Classify(const Point& p) const {
        double min_dist = p.DistanceSquare(centroids_[0]);
        size_t closest_id = 0;
        for (size_t i = 1; i < centroids_.size(); ++i) {
            double dist = p.DistanceSquare(centroids_[i]);
            if (dist < min_dist) {
                min_dist = dist;
                closest_id = i;
            }
        }
        return closest_id;
    }

    //! Calculate closest cluster to all points, returns DIA containing only the
    //! cluster ids.
    template <typename PointDIA>
    auto Classify(const PointDIA &points) const {
        return points
               .Map([this](const Point& p) { return Classify(p); });
    }

    //! Calculate closest cluster to all points, returns DIA contains pairs of
    //! points and their cluster id.
    template <typename PointDIA>
    auto ClassifyPairs(const PointDIA &points) const {
        return points
               .Map([this](const Point& p) {
                        return PointClusterId<Point>(p, Classify(p));
                    });
    }

    //! Calculate the k-means cost: the squared distance to the nearest center.
    double ComputeCost(const Point& p) const {
        double min_dist = p.DistanceSquare(centroids_[0]);
        for (size_t i = 1; i < centroids_.size(); ++i) {
            double dist = p.DistanceSquare(centroids_[i]);
            if (dist < min_dist) {
                min_dist = dist;
            }
        }
        return min_dist;
    }

    //! Calculate the overall k-means cost: the sum of squared distances to
    //! their nearest center.
    template <typename PointDIA>
    double ComputeCost(const PointDIA& points) const {
        return points
               .Map([this](const Point& p) { return ComputeCost(p); })
               .Sum();
    }

    //! \}

private:
    //! dimensions of space
    size_t dimensions_;

    //! number of clusters
    size_t num_clusters_;

    //! number of iterations
    size_t iterations_;

    //! computed centroids in cluster id order
    std::vector<Point> centroids_;
};

//! Calculate k-Means using Lloyd's Algorithm. The DIA centroids is both an
//! input and an output parameter. The method returns a std::pair<Point2D,
//! size_t> = Point2DClusterId into the centroids for each input point.
template <typename Point, typename InStack>
auto KMeans(const DIA<Point, InStack>&input_points,
            size_t dimensions, size_t num_clusters, size_t iterations) {

    auto points = input_points.Cache();

    using ClosestCentroid = ClosestCentroid<Point>;
    using CentroidAccumulated = CentroidAccumulated<Point>;

    DIA<Point> centroids = points.Sample(num_clusters);

    for (size_t iter = 0; iter < iterations; ++iter) {

        // handling this local variable is difficult: it is calculated as an
        // Action here, but must exist later when the Map() is
        // processed. Hhence, we move it into the closure.
        std::vector<Point> local_centroids = centroids.AllGather();

        // calculate the closest centroid for each point
        auto closest = points.Map(
            [local_centroids = std::move(local_centroids)](const Point& p) {
                assert(local_centroids.size());
                double min_dist = p.DistanceSquare(local_centroids[0]);
                size_t closest_id = 0;

                for (size_t i = 1; i < local_centroids.size(); ++i) {
                    double dist = p.DistanceSquare(local_centroids[i]);
                    if (dist < min_dist) {
                        min_dist = dist;
                        closest_id = i;
                    }
                }
                return ClosestCentroid {
                    closest_id, CentroidAccumulated { p, 1 }
                };
            });

        // Calculate new centroids as the mean of all points associated with it.
        centroids =
            closest
            .ReduceByKey(
                [](const ClosestCentroid& cc) { return cc.cluster_id; },
                [](const ClosestCentroid& a, const ClosestCentroid& b) {
                    return ClosestCentroid {
                        a.cluster_id,
                        CentroidAccumulated { a.center.p + b.center.p,
                                              a.center.count + b.center.count }
                    };
                })
            .Map([](const ClosestCentroid& cc) {
                     return cc.center.p / static_cast<double>(cc.center.count);
                 })
            .Collapse();
    }

    return KMeansModel<Point>(
        dimensions, num_clusters, iterations,
        centroids.AllGather());
}

} // namespace k_means
} // namespace examples

#endif // !THRILL_EXAMPLES_K_MEANS_K_MEANS_HEADER

/******************************************************************************/
