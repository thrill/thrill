/*******************************************************************************
 * tests/net/flow_control_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/net/dispatcher.hpp>
#include <thrill/net/flow_control_channel.hpp>
#include <thrill/net/flow_control_manager.hpp>
#include <thrill/net/group.hpp>
#include <thrill/net/manager.hpp>

#include <functional>
#include <string>
#include <thread>
#include <vector>

using namespace thrill::net;

/**
 * Calculates a prefix sum over all worker ids.
 */
static void SingleThreadPrefixSum(tcp::Group* net) {
    FlowControlChannelManager manager(*net, 1);
    FlowControlChannel& channel = manager.GetFlowControlChannel(0);
    size_t myRank = net->my_host_rank();

    size_t sum = channel.PrefixSum(myRank);

    size_t expected = 0;
    for (size_t i = 0; i <= myRank; i++) {
        expected += i;
    }

    ASSERT_EQ(sum, expected);
}

/**
 * Broadcasts the ID of the master, which is 0.
 */
static void SingleThreadBroadcast(tcp::Group* net) {
    FlowControlChannelManager manager(*net, 1);
    FlowControlChannel& channel = manager.GetFlowControlChannel(0);
    size_t magic = 1337;
    size_t myRank = net->my_host_rank();
    size_t value = myRank + magic;

    size_t res = channel.Broadcast(value);

    ASSERT_EQ(res, magic);
}

static void ExecuteMultiThreads(
    tcp::Group* net, size_t count, std::function<void(FlowControlChannel&, size_t)> function) {

    std::vector<std::thread> threads(count);
    FlowControlChannelManager manager(*net, count);

    for (size_t i = 0; i < count; i++) {
        threads[i] = std::thread([i, function, &manager] {
                                     function(manager.GetFlowControlChannel(i), i);
                                 });
    }

    for (size_t i = 0; i < count; i++) {
        threads[i].join();
    }
}

/**
 * Broadcasts the ID of the master, which is 0.
 */
static void MultiThreadBroadcast(tcp::Group* net) {
    const size_t count = 4;
    const size_t magic = 1337;
    ExecuteMultiThreads(
        net, count, [=](FlowControlChannel& channel, size_t id) {
            size_t myRank = net->my_host_rank() * count + id + magic;

            size_t res = channel.Broadcast(myRank);

            ASSERT_EQ(res, magic);
        });
}

/**
 * Calculates a sum over all worker ids.
 */
static void SingleThreadAllReduce(tcp::Group* net) {
    FlowControlChannelManager manager(*net, 1);
    FlowControlChannel& channel = manager.GetFlowControlChannel(0);

    size_t myRank = net->my_host_rank();

    size_t res = channel.AllReduce(myRank);

    size_t expected = 0;
    for (size_t i = 0; i < net->num_hosts(); i++) {
        expected += i;
    }

    ASSERT_EQ(res, expected);
}

/**
 * Calculates a sum over all worker and thread ids.
 */
static void MultiThreadAllReduce(tcp::Group* net) {

    const size_t count = 4;

    ExecuteMultiThreads(
        net, count, [=](FlowControlChannel& channel, size_t id) {
            size_t myRank = net->my_host_rank() * count + id;

            size_t res = channel.AllReduce(myRank);
            size_t expected = 0;
            for (size_t i = 0; i < net->num_hosts() * count; i++) {
                expected += i;
            }

            ASSERT_EQ(res, expected);
        });
}

/**
 * Calculates a sum over all worker and thread ids.
 */
static void MultiThreadPrefixSum(tcp::Group* net) {

    const size_t count = 4;

    ExecuteMultiThreads(
        net, count, [=](FlowControlChannel& channel, size_t id) {
            size_t myRank = net->my_host_rank() * count + id;

            size_t res = channel.PrefixSum(myRank);
            size_t expected = 0;
            for (size_t i = 0; i <= net->my_host_rank() * count + id; i++) {
                expected += i;
            }

            ASSERT_EQ(res, expected);
        });
}

/**
 * Does a lot of operations to provoke race contitions.
 */
static void DoLotsOfStuff(tcp::Group* net) {

    const size_t count = 16;

    ExecuteMultiThreads(
        net, count, [=](FlowControlChannel& channel, size_t id) {
            size_t myRank = net->my_host_rank() * count + id;
            std::vector<size_t> pres;
            std::vector<size_t> rres;

            for (int i = 0; i < 20; i++) {
                // Make a prefix sum and push res
                pres.push_back(channel.PrefixSum(myRank));
                // Make an all reduce and push res.
                rres.push_back(channel.AllReduce(myRank));

                // Assert that broadcast gives us the result of the master
                size_t bRes = channel.Broadcast(i + net->my_host_rank());
                ASSERT_EQ(bRes, i);
            }
            size_t pexpected = 0;
            for (size_t i = 0; i <= net->my_host_rank() * count + id; i++) {
                pexpected += i;
            }
            size_t rexpected = 0;
            for (size_t i = 0; i < net->num_hosts() * count; i++) {
                rexpected += i;
            }

            for (size_t i = 0; i < pres.size(); i++) {
                ASSERT_EQ(pexpected, pres[i]);
                ASSERT_EQ(rexpected, rres[i]);
            }
        });
}

TEST(Group, PrefixSum) {
    tcp::Group::ExecuteLocalMock(6, SingleThreadPrefixSum);
}

TEST(Group, MultiThreadPrefixSum) {
    tcp::Group::ExecuteLocalMock(6, MultiThreadPrefixSum);
}

TEST(Group, Broadcast) {
    tcp::Group::ExecuteLocalMock(6, SingleThreadBroadcast);
}

TEST(Group, MultiThreadBroadcast) {
    tcp::Group::ExecuteLocalMock(6, MultiThreadBroadcast);
}

TEST(Group, AllReduce) {
    tcp::Group::ExecuteLocalMock(6, SingleThreadAllReduce);
}

TEST(Group, MultiThreadAllReduce) {
    tcp::Group::ExecuteLocalMock(6, MultiThreadAllReduce);
}

TEST(Group, HardcoreRaceConditionTest) {
    tcp::Group::ExecuteLocalMock(6, DoLotsOfStuff);
}

/******************************************************************************/
