/*******************************************************************************
 * tests/common/reservoir_sampling_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/reservoir_sampling.hpp>

#include <thrill/common/aggregate.hpp>
#include <thrill/common/logger.hpp>
#include <tlx/die.hpp>

#include <gtest/gtest.h>

#include <fstream>
#include <vector>

using namespace thrill;

TEST(ReservoirSampling, Simple) {

    // item range inserted
    static const size_t range = 100000;
    // number of rounds for histogram
    static const size_t rounds = 1000;
    // store reservoir size
    size_t rs_size = 0;

    std::default_random_engine rng(std::random_device { } ());
    std::vector<size_t> histogram(range);

    for (size_t r = 0; r < rounds; ++r)
    {
        std::vector<size_t> samples;
        common::ReservoirSamplingGrow<size_t> rs(samples, rng);

        for (size_t i = 0; i < range; ++i)
            rs.add(i);

        for (const auto& x : rs.samples())
            histogram[x]++;

        rs_size = rs.size();
    }

    double target =
        static_cast<double>(rs_size * rounds) / static_cast<double>(range);

    common::Aggregate<double> aggr;

    for (size_t i = 0; i < histogram.size(); ++i) {
        aggr.Add((histogram[i] - target) / target);
    }

    sLOG1 << "target" << target
          << "mean" << aggr.Mean() << "stdev" << aggr.StDev()
          << "min" << aggr.Min() << "max" << aggr.Max();

    ASSERT_LT(aggr.Mean(), 0.1);
    ASSERT_LT(aggr.StDev(), 1.0);

    // std::ofstream of("data.txt");
    // for (size_t i = 0; i < histogram.size(); ++i) {
    //     // LOG1 << "histogram[" << i << "] = " << histogram[i];
    //     of << i << '\t' << histogram[i] << '\n';
    // }
}

/******************************************************************************/
