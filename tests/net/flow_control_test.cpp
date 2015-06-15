/*******************************************************************************
 * tests/net/group_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
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
    int myRank = (int)net->MyRank();

    int sum = channel.PrefixSum(myRank);

    int expected = 0;
    for(int i = 0; i <= myRank; i++) {
        expected += i;
    }

    ASSERT_EQ(sum, expected);
}


static void ThreadBroadcast(Group* net) {
    FlowControlChannel channel(*net);
    int myRank = (int)net->MyRank();

    int res = channel.Broadcast(myRank);

    ASSERT_EQ(res, 0);
}

static void ThreadAllReduce(Group* net) {
    FlowControlChannel channel(*net);

    int myRank = (int)net->MyRank();

    int res = channel.AllReduce(myRank);

    int expected = 0;
    for(size_t i = 0; i < net->Size(); i++) {
        expected += i;
    }

    ASSERT_EQ(res, expected);

}

TEST(Group, PrefixSum) {
    Group::ExecuteLocalMock(6, ThreadPrefixSum);
}

TEST(Group, Broadcast) {
    Group::ExecuteLocalMock(6, ThreadBroadcast);
}

TEST(Group, AllReduce) {
    Group::ExecuteLocalMock(6, ThreadAllReduce);
}


/******************************************************************************/
