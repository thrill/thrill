/*******************************************************************************
 * tests/common/stats_timer_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/stats_timer.hpp>

#include <thread>

using namespace thrill::common;

TEST(StatsTimer, Test1) {
    StatsTimerBase<true> timer1(/* autostart */ false);
    StatsTimerBase<false> timer2(/* autostart */ false);

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

/******************************************************************************/
