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
#include <thrill/common/vector.hpp>

#include <cereal/types/vector.hpp>
#include <thrill/data/serialization_cereal.hpp>

#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace examples {
namespace k_means {

using thrill::DIA;

//! Compile-Time Fixed-Dimensional Points
template <size_t D>
using Point = thrill::common::Vector<D, double>;

//! A variable D-dimensional point with double precision
using VPoint = thrill::common::VVector<double>;

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
    auto Classify(const PointDIA& points) const {
        return points
               .Map([this](const Point& p) { return Classify(p); });
    }

    //! Calculate closest cluster to all points, returns DIA contains pairs of
    //! points and their cluster id.
    template <typename PointDIA>
    auto ClassifyPairs(const PointDIA& points) const {
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
auto KMeans(const DIA<Point, InStack>& input_points, size_t dimensions,
            size_t num_clusters, size_t iterations, double epsilon) {

    auto points = input_points.Cache();

	bool break_condition = false;

    using ClosestCentroid = ClosestCentroid<Point>;
    using CentroidAccumulated = CentroidAccumulated<Point>;

	std::vector<Point> local_centroids = 
		points.Keep().Sample(num_clusters).AllGather();

    for (size_t iter = 0; iter < iterations && !break_condition; ++iter) {

		std::vector<Point> old_local_centroids(local_centroids);

        // calculate the closest centroid for each point
        auto closest = points.Keep().Map(
            [local_centroids](const Point& p) {
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
        auto centroids =
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
                     return CentroidAccumulated{
						 cc.center.p / static_cast<double>(cc.center.count),
						 cc.cluster_id };
                 })
            .Collapse();

		// Check whether centroid positions changed significantly,
		// if yes do another iteration
		break_condition = true;
		auto unsortedCentroids = centroids.AllGather();
		
		for (auto &uc : unsortedCentroids) {
			local_centroids[uc.count] = uc.p;
		}

		for (size_t i = 0; i < local_centroids.size(); ++i) {
			if (local_centroids[i].Distance(old_local_centroids[i]) > epsilon) {
				break_condition = false;
				break;
			}
		}
    }

    return KMeansModel<Point>(
        dimensions, num_clusters, iterations,
        local_centroids);
}

//! Calculate k-Means using bisecting method
template <typename Point, typename InStack>
auto BisecKMeans(const DIA<Point, InStack>& input_points, size_t dimensions,
	size_t num_clusters, size_t iterations, double epsilon) {

	using ClosestCentroid = ClosestCentroid<Point>;
	using CentroidAccumulated = CentroidAccumulated<Point>;

	//! initial cluster size
	size_t initial_size = num_clusters <= 2 ? num_clusters : 2;

	//! model that is steadily updated and returned to the calling function
	auto _result_model =
		KMeans(input_points, dimensions, initial_size, iterations, epsilon);

	for (size_t size = initial_size; size < num_clusters; ++size) {

		// Classify all points for the current k-Means model
		auto classified_points = _result_model.ClassifyPairs(input_points)
			.Map([](const PointClusterId<Point>& pci) {
			return ClosestCentroid{ pci.second,
				CentroidAccumulated{ pci.first, 1 }
			}; });

		// Create a vector containing the cluster IDs and the number 
		// of closest points in order to determine the biggest cluster
		size_t biggest_cluster_idx = classified_points
			.ReduceByKey(
				[](const ClosestCentroid& cc) { return cc.cluster_id; },
				[](const ClosestCentroid& a, const ClosestCentroid& b) {
			return ClosestCentroid{ a.cluster_id,
				CentroidAccumulated{ a.center.p,
				a.center.count + b.center.count } };
		})
			.AllReduce(
				[](const ClosestCentroid& cc1, const ClosestCentroid& cc2) {
			return cc1.center.count > cc2.center.count ? cc1 : cc2;
		})
			.cluster_id;

		// Filter the points of the biggest cluster for a further split
		auto filtered_points = classified_points
			.Filter(
				[biggest_cluster_idx](const ClosestCentroid& cc) {
			return cc.cluster_id == biggest_cluster_idx;
		})
			.Map(
				[](const ClosestCentroid& cc) {
			return cc.center.p;
		});

		// Compute two new cluster by splitting the biggest one 
		std::vector<Point> tmp_model_centroids =
			KMeans(filtered_points, dimensions, 2, iterations, epsilon)
			.centroids();

		// Delete the centroid of the biggest cluster
		// Add two new centroids calculated in the previous step 
		std::vector<Point> result_model_centroids = _result_model.centroids();
		result_model_centroids.erase(result_model_centroids.begin()
			+ biggest_cluster_idx);
		result_model_centroids.insert(result_model_centroids.end(),
			tmp_model_centroids.begin(), tmp_model_centroids.end());

		// Update centroids of the result_model
		_result_model = KMeansModel<Point>(
			dimensions, num_clusters, iterations, result_model_centroids);
	}

	return _result_model;
}

} // namespace k_means
} // namespace examples

#endif // !THRILL_EXAMPLES_K_MEANS_K_MEANS_HEADER

/******************************************************************************/
