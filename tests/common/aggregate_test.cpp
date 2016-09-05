/*******************************************************************************
 * tests/common/aggregate_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/aggregate.hpp>
#include <thrill/common/logger.hpp>

using namespace thrill;

TEST(Aggregate, Integer) {

    common::Aggregate<int> agg;

    for (int i = 0; i < 30; ++i) {
        agg.Add(i);
    }

    ASSERT_EQ(30, agg.Count());
    ASSERT_EQ((29 * 30) / 2, agg.Total());
    ASSERT_DOUBLE_EQ(14.5, agg.Average());
    ASSERT_EQ(0, agg.Min());
    ASSERT_EQ(29, agg.Max());
    ASSERT_DOUBLE_EQ(8.8034084308295046, agg.StandardDeviation());

}

TEST(Aggregate, Double) {
    common::Aggregate<double> agg;

    for (size_t i = 1; i <= 1000; ++i) {
        agg.Add(1.0/static_cast<double>(i));
    }

    ASSERT_EQ(1000, agg.Count());
    ASSERT_DOUBLE_EQ(7.4854708605503451, agg.Total());
    ASSERT_DOUBLE_EQ(0.0074854708605503447, agg.Average());
    ASSERT_EQ(0.001, agg.Min());
    ASSERT_EQ(1.0, agg.Max());
    ASSERT_DOUBLE_EQ(0.039868430925506362, agg.StandardDeviation());
    ASSERT_DOUBLE_EQ(0.039848491723996423, agg.StandardDeviation(0));

}

/******************************************************************************/
