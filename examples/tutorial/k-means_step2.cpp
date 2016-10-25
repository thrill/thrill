/*******************************************************************************
 * examples/tutorial/k-means_step2.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

//! \example examples/tutorial/k-means_step2.cpp
//!
//! This example is part of the k-means tutorial. See \ref kmeans_tutorial_step2

#include <thrill/api/all_gather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
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
};
//! make ostream-able for Print()
std::ostream& operator << (std::ostream& os, const ClosestCenter& cc) {
    return os << '(' << cc.cluster_id << ':' << cc.point << ')';
}
//! [ClosestCenter class]

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

    //! [step2 sample]
    // pick some initial random cluster centers
    auto centers = points.Sample(/* num_clusters */ 10);
    //! [step2 sample]

    //! [step2 classify]
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
            return ClosestCenter { cluster_id, p };
        });

    closest.Print("closest");
    //! [step2 classify]
}

int main() {
    // launch Thrill program: the lambda function will be run on each worker.
    return thrill::Run(
        [&](thrill::Context& ctx) { Process(ctx); });
}

/******************************************************************************/
