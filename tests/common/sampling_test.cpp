/*******************************************************************************
 * tests/common/sampling_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2017 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/sampling.hpp>

#include <thrill/common/aggregate.hpp>
#include <thrill/common/logger.hpp>
#include <tlx/die.hpp>

#include <gtest/gtest.h>

#include <fstream>
#include <numeric>
#include <vector>

using namespace thrill;

TEST(Sampling, Simple) {

    // item range inserted
    static const size_t range = 100000;
    // number of rounds for histogram
    static const size_t rounds = 10000;
    // store reservoir size
    size_t s_size = 0;

    std::mt19937 rng(std::random_device { } ());
    std::vector<size_t> histogram(range);

    std::vector<size_t> input(range);
    // fill with values [0..range)
    std::iota(input.begin(), input.end(), 0);

    for (size_t r = 0; r < rounds; ++r)
    {
        std::vector<size_t> samples;
        common::Sampling<> s(rng);
        s(input.begin(), input.end(), 1000, samples);

        for (const auto& x : samples)
            histogram[x]++;

        s_size = samples.size();
    }

    double target =
        static_cast<double>(s_size * rounds) / static_cast<double>(range);

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
