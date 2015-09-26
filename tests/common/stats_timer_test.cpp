/*******************************************************************************
 * tests/common/stats_timer_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/stats.hpp> //for forced instantiation below
#include <thrill/common/stats_timer.hpp>

#include <thread>

using namespace thrill::common;

TEST(StatsTimer, Test1) {
    StatsTimer<true> timer1;
    StatsTimer<false> timer2;

    timer1.Start();
    timer2.Start();

    // sleep just once
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    timer1.Stop();
    timer2.Stop();

    ASSERT_EQ(timer1.Real(), true);
    ASSERT_EQ(timer2.Real(), false);

    ASSERT_GT(timer1.Microseconds(), 150000);
    ASSERT_EQ(timer2.Microseconds(), 0);

    ASSERT_GT(timer1.Milliseconds(), 150);
    ASSERT_EQ(timer2.Milliseconds(), 0);

    ASSERT_GE(timer1.Seconds(), 0);
    ASSERT_EQ(timer2.Seconds(), 0);
}

namespace thrill {
namespace common {

// forced instantiations
template class StatsTimer<true>;
template class StatsTimer<false>;
template class Stats<true>;
template class Stats<false>;

} // namespace common
} // namespace thrill

/******************************************************************************/
