/*******************************************************************************
 * tests/common/stats_counter_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/stats_counter.hpp>
#include <gtest/gtest.h>

using namespace c7a::common;

TEST(StatsCounter, Test1) {
    StatsCounter<long, true> counter;
    counter++;
    ++counter;
    counter += 40;

    ASSERT_EQ(counter.Real(), true);
    ASSERT_EQ(counter, 42);

    StatsCounter<long, true> counter2 = counter;

    counter2.set_max(40);
    ASSERT_EQ(counter2, 42);
}

TEST(StatsCounter, Test2) {
    StatsCounter<long, false> counter;
    counter++;
    ++counter;
    counter += 40;

    ASSERT_EQ(counter.Real(), false);
    ASSERT_EQ(counter, 0);

    StatsCounter<long, false> counter2 = counter;

    counter2.set_max(40);
    ASSERT_EQ(counter2, 0);
}

namespace c7a {
namespace common {

// forced instantiations
template class StatsCounter<long, true>;
template class StatsCounter<long, false>;

} // namespace common
} // namespace c7a

/******************************************************************************/
