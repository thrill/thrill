/*******************************************************************************
 * examples/tutorial/k-means_step3.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

//! \example examples/tutorial/k-means_step3.cpp
//!
//! This example is part of the k-means tutorial. See \ref kmeans_tutorial_step3

#include <thrill/api/all_gather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/sample.hpp>

#include <ostream>
#include <random>
#include <vector>

//! [Point class]
//! A 2-dimensional point with double precision
struct Point {
    //! point coordinates
    double x, y;

    double DistanceSquare(const Point& b) const {
        return (x - b.x) * (x - b.x) + (y - b.y) * (y - b.y);
    }
    Point operator + (const Point& b) const {
        return Point { x + b.x, y + b.y };
    }
    Point operator / (double s) const {
        return Point { x / s, y / s };
    }
};
//! [Point class]

//! make ostream-able for Print()
std::ostream& operator << (std::ostream& os, const Point& p) {
    return os << '(' << p.x << ',' << p.y << ')';
}

//! [ClosestCenter class]
//! Assignment of a point to a cluster.
struct ClosestCenter {
    size_t cluster_id;
    Point  point;
    size_t count;
};
//! make ostream-able for Print()
std::ostream& operator << (std::ostream& os, const ClosestCenter& cc) {
    return os << '(' << cc.cluster_id
              << ':' << cc.point << ':' << cc.count << ')';
}
//! [ClosestCenter class]

//! our main processing method
void Process(thrill::Context& ctx) {

    std::default_random_engine rng(std::random_device { } ());
    std::uniform_real_distribution<double> dist(0.0, 1000.0);

    // generate 100 random points using uniform distribution
    auto points =
        Generate(
            ctx, /* size */ 100,
            [&](const size_t&) {
                return Point { dist(rng), dist(rng) };
            })
        .Cache();

    // print out the points
    points.Print("points");

    // pick some initial random cluster centers
    auto centers = points.Sample(/* num_clusters */ 10);

    // collect centers in a local vector on each worker
    std::vector<Point> local_centers = centers.AllGather();

    // calculate the closest center for each point
    auto closest = points.Map(
        [local_centers](const Point& p) {
            double min_dist = p.DistanceSquare(local_centers[0]);
            size_t cluster_id = 0;

            for (size_t i = 1; i < local_centers.size(); ++i) {
                double dist = p.DistanceSquare(local_centers[i]);
                if (dist < min_dist)
                    min_dist = dist, cluster_id = i;
            }
            //! [step3 count 1]
            return ClosestCenter { cluster_id, p, /* count */ 1 };
            //! [step3 count 1]
        });

    closest.Print("closest");

    //! [step3 ReduceByKey]
    // calculate new centers as the mean of all points associated with its id
    auto reduced_centers =
        closest
        .ReduceByKey(
            // key extractor: the cluster id
            [](const ClosestCenter& cc) { return cc.cluster_id; },
            // reduction: add points and the counter
            [](const ClosestCenter& a, const ClosestCenter& b) {
                return ClosestCenter {
                    a.cluster_id, a.point + b.point, a.count + b.count
                };
            });

    reduced_centers.Print("reduced_centers");
    //! [step3 ReduceByKey]

    //! [step3 ReduceByKey divide by count]
    auto new_centers =
        reduced_centers
        .Map([](const ClosestCenter& cc) {
                 return cc.point / cc.count;
             });

    new_centers.Print("new_centers");
    //! [step3 ReduceByKey divide by count]
}

int main() {
    // launch Thrill program: the lambda function will be run on each worker.
    return thrill::Run(
        [&](thrill::Context& ctx) { Process(ctx); });
}

/******************************************************************************/
