/*******************************************************************************
 * tests/net/flow_control_test_base.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_TESTS_NET_FLOW_CONTROL_TEST_BASE_HEADER
#define THRILL_TESTS_NET_FLOW_CONTROL_TEST_BASE_HEADER

#include <gtest/gtest.h>
#include <thrill/net/dispatcher.hpp>
#include <thrill/net/flow_control_channel.hpp>
#include <thrill/net/flow_control_manager.hpp>
#include <thrill/net/group.hpp>
#include <thrill/net/tcp/group.hpp>

#include <functional>
#include <string>
#include <thread>
#include <vector>

using namespace thrill; // NOLINT

/**
 * Calculates a prefix sum over all worker ids.
 */
static void TestSingleThreadPrefixSum(net::Group* net) {
    net::FlowControlChannelManager manager(*net, 1);
    net::FlowControlChannel& channel = manager.GetFlowControlChannel(0);
    size_t myRank = net->my_host_rank();
    size_t zero = 0;

    size_t resInclusive = channel.PrefixSum(myRank, zero, std::plus<size_t>(), true);
    size_t resExclusive = channel.PrefixSum(myRank, zero, std::plus<size_t>(), false);
    size_t expectedInclusive = 0;
    size_t expectedExclusive = 0;

    for (size_t i = 0; i <= myRank; i++) {
        expectedInclusive += i;
    }

    for (size_t i = 0; i < myRank; i++) {
        expectedExclusive += i;
    }

    ASSERT_EQ(expectedInclusive, resInclusive);
    ASSERT_EQ(expectedExclusive, resExclusive);
}

/**
 * Broadcasts the ID of the master, which is 0.
 */
static void TestSingleThreadBroadcast(net::Group* net) {
    net::FlowControlChannelManager manager(*net, 1);
    net::FlowControlChannel& channel = manager.GetFlowControlChannel(0);
    size_t magic = 1337;
    size_t myRank = net->my_host_rank();
    size_t value = myRank + magic;

    size_t res = channel.Broadcast(value);

    ASSERT_EQ(res, magic);
}

static void ExecuteMultiThreads(
    net::Group* net, size_t count,
    const std::function<void(net::FlowControlChannel&, size_t)>& function) {

    std::vector<std::thread> threads(count);
    net::FlowControlChannelManager manager(*net, count);

    for (size_t i = 0; i < count; i++) {
        threads[i] = std::thread(
            [i, function, &manager] {
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
static void TestMultiThreadBroadcast(net::Group* net) {
    const size_t count = 4;
    const size_t magic = 1337;
    ExecuteMultiThreads(
        net, count, [=](net::FlowControlChannel& channel, size_t id) {
            size_t myRank = net->my_host_rank() * count + id + magic;

            size_t res = channel.Broadcast(myRank);

            ASSERT_EQ(res, magic);
        });
}

/**
 * Calculates a sum over all worker ids.
 */
static void TestSingleThreadAllReduce(net::Group* net) {
    net::FlowControlChannelManager manager(*net, 1);
    net::FlowControlChannel& channel = manager.GetFlowControlChannel(0);

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
static void TestMultiThreadAllReduce(net::Group* net) {

    const size_t count = 4;

    ExecuteMultiThreads(
        net, count, [=](net::FlowControlChannel& channel, size_t id) {
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
static void TestMultiThreadPrefixSum(net::Group* net) {

    const size_t count = 4;

    ExecuteMultiThreads(
        net, count, [=](net::FlowControlChannel& channel, size_t id) {
            size_t myRank = net->my_host_rank() * count + id;
            size_t zero = 0;

            size_t resInclusive = channel.PrefixSum(myRank, zero, std::plus<size_t>(), true);
            size_t resExclusive = channel.PrefixSum(myRank, zero, std::plus<size_t>(), false);
            size_t expectedInclusive = 0;
            size_t expectedExclusive = 0;

            for (size_t i = 0; i <= myRank; i++) {
                expectedInclusive += i;
            }

            for (size_t i = 0; i < myRank; i++) {
                expectedExclusive += i;
            }

            ASSERT_EQ(expectedInclusive, resInclusive);
            ASSERT_EQ(expectedExclusive, resExclusive);
        });
}

/**
 * Does a lot of operations to provoke race conditions.
 */
static void TestHardcoreRaceConditionTest(net::Group* net) {

    const size_t count = 16;

    ExecuteMultiThreads(
        net, count, [=](net::FlowControlChannel& channel, size_t id) {
            size_t myRank = net->my_host_rank() * count + id;
            size_t zero = 0;
            std::vector<size_t> pres;
            std::vector<size_t> rres;

            for (int i = 0; i < 20; i++) {
                // Make a prefix sum and push res
                pres.push_back(channel.PrefixSum(myRank, zero));
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

#endif // !THRILL_TESTS_NET_FLOW_CONTROL_TEST_BASE_HEADER

/******************************************************************************/
