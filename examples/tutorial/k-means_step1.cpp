/*******************************************************************************
 * examples/tutorial/k-means_step1.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

//! \example examples/tutorial/k-means_step1.cpp
//!
//! This example is part of the k-means tutorial. See \ref kmeans_tutorial_step1

#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>

#include <ostream>
#include <random>

using thrill::DIA;

//! [Point class]
//! A 2-dimensional point with double precision
struct Point {
    //! point coordinates
    double x, y;
};
//! [Point class]

//! [Point ostream]
//! make ostream-able for Print()
std::ostream& operator << (std::ostream& os, const Point& p) {
    return os << '(' << p.x << ',' << p.y << ')';
}
//! [Point ostream]

//! [our main processing method]
void Process(thrill::Context& ctx) {

    std::default_random_engine rng(std::random_device { } ());
    std::uniform_real_distribution<double> dist(0.0, 1000.0);

    // generate 100 random points using uniform distribution
    DIA<Point> points =
        Generate(
            ctx, /* size */ 100,
            [&](const size_t& /* index */) {
                return Point { dist(rng), dist(rng) };
            })
        .Cache();

    // print out the points
    points.Print("points");
}
//! [our main processing method]

//! [Thrill Run launcher]
int main() {
    // launch Thrill program: the lambda function will be run on each worker.
    return thrill::Run(
        [&](thrill::Context& ctx) { Process(ctx); });
}
//! [Thrill Run launcher]

/******************************************************************************/
