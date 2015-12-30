/*******************************************************************************
 * examples/kmeans.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_KMEANS_HEADER
#define THRILL_EXAMPLES_KMEANS_HEADER

#include <thrill/api/allgather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/groupby.hpp>

#include <limits>
#include <string>
#include <vector>

using thrill::DIA;
using thrill::Context;

using namespace thrill; // NOLINT

namespace examples {

using Point2D = std::pair<float, float>;
using Centrioid = Point2D;
using ClosestCentroid = std::pair<Centrioid, Point2D>;
using CentroidAcc = std::pair<Centrioid, size_t>;

template <typename InStack>
auto kMeans(const DIA<std::string, InStack>&in1,
            const DIA<std::string, InStack>&in2) {

    auto points = in1.Map(
        [](const std::string& input) {
            auto split = thrill::common::Split(input, " ");
            return Point2D(std::stof(split[0]),
                           std::stof(split[1]));
        }).Cache();

    DIA<Centrioid> centroids = in2.Map(
        [](const std::string& input) {
            auto split = thrill::common::Split(input, " ");
            return Centrioid(std::stof(split[0]),
                             std::stof(split[1]));
        }).Cache();

    std::vector<Centrioid> cs = centroids.AllGather();

    // do iterations
    for (int i = 0; i < 100; ++i) { // Todo(ms): add threshold-based termination

        DIA<ClosestCentroid> closest = points.Map(
            [&](const Point2D& p) {

                float minDistance = std::numeric_limits<float>::max();
                Centrioid closestCentroid;

                for (Centrioid c: cs) {
                    float distance = sqrt(pow(p.first - c.first, 2) + pow(p.second - c.second, 2));
                    if (distance < minDistance) {
                        minDistance = distance;
                        closestCentroid = c;
                    }
                }

                return ClosestCentroid(closestCentroid, p);
            }).Cache();

        auto accs = closest.GroupBy<CentroidAcc>(
            [](ClosestCentroid p) { return 0.5 * (p.first.first + p.first.second)
                                    * (p.first.first + p.first.second + 1.0) + p.first.second;
            },
            [](auto& r, float) {
                Centrioid accPoint(0.0, 0.0);
                size_t sum = 0;
                while (r.HasNext()) {
                    ClosestCentroid c = r.Next();
                    accPoint.first += c.second.first;
                    accPoint.second += c.second.second;
                    sum++;
                }

                return CentroidAcc(accPoint, sum);
            });

        centroids = accs.Map(
            [](const CentroidAcc& a) {
                return Centrioid(a.first.first / a.second, a.first.second / a.second);
            });

        cs = centroids.AllGather();
    }

    return centroids;
}
} // namespace examples

#endif // !THRILL_EXAMPLES_KMEANS_HEADER

/******************************************************************************/
