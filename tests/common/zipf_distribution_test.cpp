/*******************************************************************************
 * tests/common/zipf_distribution_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/zipf_distribution.hpp>

#include <thrill/common/logger.hpp>

#include <gtest/gtest.h>
#include <map>

using namespace thrill;

TEST(ZipfDistribution, Simple) {
    static const bool debug = false;

    common::ZipfDistribution zipf(1000, 0.3, 0.5);

    std::default_random_engine rng(std::random_device { } ());

    std::map<size_t, size_t> countmap;

    for (size_t i = 0; i < 10000; ++i) {
        countmap[zipf(rng)]++;
    }

    for (const auto& e : countmap) {
        LOG << "freq[" << e.first << "] = " << e.second;
    }
}

/******************************************************************************/
