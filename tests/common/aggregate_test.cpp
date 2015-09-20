/*******************************************************************************
 * tests/common/aggregate_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/aggregate.hpp>
#include <thrill/common/logger.hpp>

using namespace thrill;

TEST(Aggregate, Test1) {

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

/******************************************************************************/
