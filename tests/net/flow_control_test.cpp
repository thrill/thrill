/*******************************************************************************
 * tests/net/flow_control_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/net/dispatcher.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>
#include <c7a/net/group.hpp>
#include <c7a/net/manager.hpp>
#include <gtest/gtest.h>

#include <functional>
#include <string>
#include <thread>
#include <vector>

using namespace c7a::net;

/**
 * Calculates a prefix sum over all worker ids.
 */
static void SingleThreadPrefixSum(Group* net) {
    FlowControlChannelManager manager(*net, 1);
    FlowControlChannel& channel = manager.GetFlowControlChannel(0);
    int myRank = (int)net->my_host_rank();

    int sum = channel.PrefixSum(myRank);

    int expected = 0;
    for (int i = 0; i <= myRank; i++) {
        expected += i;
    }

    ASSERT_EQ(sum, expected);
}

/**
 * Broadcasts the ID of the master, which is 0.
 */
static void SingleThreadBroadcast(Group* net) {
    FlowControlChannelManager manager(*net, 1);
    FlowControlChannel& channel = manager.GetFlowControlChannel(0);
    int magic = 1337;
    int myRank = (int)net->my_host_rank();
    int value = myRank + magic;

    int res = channel.Broadcast(value);

    ASSERT_EQ(res, magic);
}

static void ExecuteMultiThreads(Group* net, int count, std::function<void(FlowControlChannel&, int)> function) {

    std::vector<std::thread> threads(count);
    FlowControlChannelManager manager(*net, count);

    for (int i = 0; i < count; i++) {
        threads[i] = std::thread([i, function, &manager] {
                                     function(manager.GetFlowControlChannel(i), i);
                                 });
    }

    for (int i = 0; i < count; i++) {
        threads[i].join();
    }
}

/**
 * Broadcasts the ID of the master, which is 0.
 */
static void MultiThreadBroadcast(Group* net) {
    const int count = 4;
    const int magic = 1337;
    ExecuteMultiThreads(net, count, [=](FlowControlChannel& channel, int id) {
                            int myRank = (int)net->my_host_rank() * count + id + magic;

                            int res = channel.Broadcast(myRank);

                            ASSERT_EQ(res, magic);
                        });
}

/**
 * Calculates a sum over all worker ids.
 */
static void SingleThreadAllReduce(Group* net) {
    FlowControlChannelManager manager(*net, 1);
    FlowControlChannel& channel = manager.GetFlowControlChannel(0);

    int myRank = (int)net->my_host_rank();

    int res = channel.AllReduce(myRank);

    int expected = 0;
    for (size_t i = 0; i < net->num_hosts(); i++) {
        expected += i;
    }

    ASSERT_EQ(res, expected);
}

/**
 * Calculates a sum over all worker and thread ids.
 */
static void MultiThreadAllReduce(Group* net) {

    const int count = 4;

    ExecuteMultiThreads(net, count, [=](FlowControlChannel& channel, int id) {
                            int myRank = (int)net->my_host_rank() * count + id;

                            int res = channel.AllReduce(myRank);
                            int expected = 0;
                            for (size_t i = 0; i < net->num_hosts() * count; i++) {
                                expected += i;
                            }

                            ASSERT_EQ(res, expected);
                        });
}

/**
 * Calculates a sum over all worker and thread ids.
 */
static void MultiThreadPrefixSum(Group* net) {

    const int count = 4;

    ExecuteMultiThreads(net, count, [=](FlowControlChannel& channel, int id) {
                            int myRank = (int)net->my_host_rank() * count + id;

                            int res = channel.PrefixSum(myRank);
                            int expected = 0;
                            for (size_t i = 0; i <= net->my_host_rank() * count + id; i++) {
                                expected += i;
                            }

                            ASSERT_EQ(res, expected);
                        });
}

TEST(Group, PrefixSum) {
    Group::ExecuteLocalMock(6, SingleThreadPrefixSum);
}

TEST(Group, MultiThreadPrefixSum) {
    Group::ExecuteLocalMock(6, MultiThreadPrefixSum);
}

TEST(Group, Broadcast) {
    Group::ExecuteLocalMock(6, SingleThreadBroadcast);
}

TEST(Group, MultiThreadBroadcast) {
    Group::ExecuteLocalMock(6, MultiThreadBroadcast);
}

TEST(Group, AllReduce) {
    Group::ExecuteLocalMock(6, SingleThreadAllReduce);
}

TEST(Group, MultiThreadAllReduce) {
    Group::ExecuteLocalMock(6, MultiThreadAllReduce);
}

/******************************************************************************/
