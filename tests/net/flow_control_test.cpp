/*******************************************************************************
 * tests/net/group_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/net/group.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/dispatcher.hpp>
#include <c7a/net/manager.hpp>
#include <gtest/gtest.h>

#include <thread>
#include <vector>
#include <string>
#include <random>

using namespace c7a::net;

static void ThreadPrefixSum(Group* net) {
    FlowControlChannel channel(*net);

    int sum = channel.PrefixSum(net->MyRank(), [](int a, int b) { return a + b });

    int expected = 0;
    for(int i = 0; i <= net->MyRank(); i++) {
        expected += i;
    }

    ASSERT_EQ(sum, expected);
}


TEST(Group, PrefixSum) {
    // Construct a Group of 6 workers which execute the thread function
    // above which sends and receives a message from all workers.
    Group::ExecuteLocalMock(6, ThreadPrefixSum);
}

/******************************************************************************/
