/*******************************************************************************
 * examples/tutorial/k-means_step5.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

//! \example examples/tutorial/k-means_step5.cpp
//!
//! This example is part of the k-means tutorial. See \ref kmeans_tutorial_step5

#include <thrill/api/all_gather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/sample.hpp>
#include <thrill/api/write_lines.hpp>

#include <ostream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

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

//! make ostream-able for Print()
std::ostream& operator << (std::ostream& os, const Point& p) {
    return os << '(' << p.x << ',' << p.y << ')';
}

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

//! [step5 new Process signature]
//! our main processing method
void Process(const thrill::DIA<Point>& points, const char* output) {
//! [step5 new Process signature]

    // print out the points
    // points.Print("points");

    // pick some initial random cluster centers
    thrill::DIA<Point> centers = points.Sample(/* num_clusters */ 10);

    for (size_t iter = 0; iter < /* iterations */ 10; ++iter)
    {
        // collect centers in a local vector on each worker
        std::vector<Point> local_centers = centers.AllGather();

        auto new_centers =
            points
            // calculate the closest center for each point
            .Map(
                [local_centers](const Point& p) {
                    double min_dist = p.DistanceSquare(local_centers[0]);
                    size_t cluster_id = 0;

                    for (size_t i = 1; i < local_centers.size(); ++i) {
                        double dist = p.DistanceSquare(local_centers[i]);
                        if (dist < min_dist)
                            min_dist = dist, cluster_id = i;
                    }
                    return ClosestCenter { cluster_id, p, /* count */ 1 };
                })
            // new centers as the mean of all points associated with it
            .ReduceByKey(
                // key extractor: the cluster id
                [](const ClosestCenter& cc) { return cc.cluster_id; },
                // reduction: add points and the counter
                [](const ClosestCenter& a, const ClosestCenter& b) {
                    return ClosestCenter {
                        a.cluster_id, a.point + b.point, a.count + b.count
                    };
                })
            .Map([](const ClosestCenter& cc) {
                     return cc.point / cc.count;
                 });

        // new_centers.Print("new_centers");

        // Collapse() is needed to fold lambda chain to DIA<Points>
        centers = new_centers.Collapse();
    }

    //! [step5 WriteLines output]
    if (output) {
        // write output as "x y" lines
        centers
        .Map([](const Point& p) {
                 return std::to_string(p.x) + " " + std::to_string(p.y);
             })
        .WriteLines(output);
    }
    else {
        centers.Print("final centers");
    }
    //! [step5 WriteLines output]
}

//! [step5 GeneratePoints]
thrill::DIA<Point> GeneratePoints(thrill::Context& ctx) {
    std::default_random_engine rng(std::random_device { } ());
    std::uniform_real_distribution<double> dist(0.0, 1000.0);

    // generate 100 random points using uniform distribution
    auto points =
        Generate(
            ctx, /* size */ 100,
            [&](const size_t&) {
                return Point { dist(rng), dist(rng) };
            });
    // Execute() is require due to lazy evaluation
    return points.Cache().Execute();
}
//! [step5 GeneratePoints]

//! [step5 LoadPoints]
thrill::DIA<Point> LoadPoints(thrill::Context& ctx, const char* path) {
    // load points from text file
    auto points =
        ReadLines(ctx, path)
        .Map(
            [](const std::string& input) {
                // parse "<x> <y>" lines
                std::istringstream iss(input);
                Point p;
                iss >> p.x >> p.y;
                if (iss.peek() != EOF)
                    die("Could not parse point coordinates: " << input);
                return p;
            });
    return points.Cache();
}
//! [step5 LoadPoints]

//! [step5 Run launcher]
int main(int argc, char* argv[]) {
    // launch Thrill program: the lambda function will be run on each worker.
    return thrill::Run(
        [&](thrill::Context& ctx) {
            if (argc == 1)
                Process(GeneratePoints(ctx), nullptr);
            else if (argc == 2)
                Process(LoadPoints(ctx, argv[1]), nullptr);
            else if (argc == 3)
                Process(LoadPoints(ctx, argv[1]), argv[2]);
            else
                std::cerr << "Usage: " << argv[0]
                          << " [points] [output]" << std::endl;
        });
}
//! [step5 Run launcher]

/******************************************************************************/
