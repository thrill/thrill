/*******************************************************************************
 * tests/net/dispatcher_thread_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/net/dispatcher_thread.hpp>
#include <gtest/gtest.h>

using c7a::net::DispatcherThread;

TEST(DispatcherThread, Test1) {

    DispatcherThread disp;
    disp.Start();
    std::this_thread::sleep_for(std::chrono::nanoseconds(100));
    disp.Stop();
}

/******************************************************************************/
