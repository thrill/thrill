/*******************************************************************************
 * tests/net/group_test_base.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_TESTS_NET_GROUP_TEST_BASE_HEADER
#define THRILL_TESTS_NET_GROUP_TEST_BASE_HEADER

#include <thrill/common/math.hpp>
#include <thrill/net/group.hpp>

#include <string>

using namespace thrill;      // NOLINT

//! do nothing but terminate, this check construction and destruction.
static void TestNoOperation(net::Group*) { }

//! send and receive a message from both neighbors.
static void TestSendRecvCyclic(net::Group* net) {

    size_t id = net->my_host_rank();

    if (id != 0) {
        size_t res;
        net->ReceiveFrom<size_t>(id - 1, &res);
        ASSERT_EQ(id - 1, res);
    }

    if (id != net->num_hosts() - 1) {
        net->SendTo(id + 1, id);
    }
}

//! sends and receives a POD message from all workers.
static void TestBroadcastIntegral(net::Group* net) {
    static const bool debug = false;

    // Broadcast our ID to everyone
    for (size_t i = 0; i != net->num_hosts(); ++i)
    {
        if (i == net->my_host_rank()) continue;
        net->SendTo(i, net->my_host_rank());
    }

    // Receive the id from everyone. Make sure that the id is correct.
    for (size_t i = 0; i != net->num_hosts(); ++i)
    {
        if (i == net->my_host_rank()) continue;

        size_t val;

        net->ReceiveFrom<size_t>(i, &val);
        LOG << "Received " << val << " from " << i;
        ASSERT_EQ(i, val);
    }
}

//! sends and receives a String message from all workers.
static void TestSendReceiveAll2All(net::Group* net) {
    static const bool debug = false;

    // send a message to all other clients except ourselves.
    for (size_t i = 0; i != net->num_hosts(); ++i)
    {
        if (i == net->my_host_rank()) continue;
        net->SendTo(i, "Hello " + std::to_string(net->my_host_rank())
                    + " -> " + std::to_string(i));
    }
    // receive the n-1 messages from clients in order
    for (size_t i = 0; i != net->num_hosts(); ++i)
    {
        if (i == net->my_host_rank()) continue;

        std::string msg;
        net->ReceiveFrom(i, &msg);
        sLOG << "Received from client" << i << "msg" << msg;

        ASSERT_EQ(msg, "Hello " + std::to_string(i)
                  + " -> " + std::to_string(net->my_host_rank()));
    }
}

//! sends and receives asynchronous messages between all workers.
template <typename Dispatcher>
static void DispatcherTestSyncSendAsyncRead(net::Group* net) {
    // send a message to all other clients except ourselves.
    for (size_t i = 0; i != net->num_hosts(); ++i)
    {
        if (i == net->my_host_rank()) continue;
        net->connection(i).SyncSend(&i, sizeof(size_t));
    }

    size_t received = 0;
    mem::Manager mem_manager(nullptr, "Dispatcher");
    Dispatcher dispatcher(mem_manager);

    net::AsyncReadCallback callback =
        [net, &received](net::Connection& /* s */, const net::Buffer& buffer) {
            ASSERT_EQ(*(reinterpret_cast<const size_t*>(buffer.data())),
                      net->my_host_rank());
            received++;
        };

    // add async reads to net dispatcher
    for (size_t i = 0; i != net->num_hosts(); ++i)
    {
        if (i == net->my_host_rank()) continue;
        dispatcher.AsyncRead(net->connection(i), sizeof(size_t), callback);
    }

    while (received < net->num_hosts() - 1) {
        dispatcher.Dispatch();
    }
}

//! let group of p hosts perform a PrefixSum collective
static void TestPrefixSumForPowersOfTwo(net::Group* net) {
    // only for powers of two

// if (net->num_hosts() != common::RoundUpToPowerOfTwo(net->num_hosts()))
//    return;

    size_t local_value = 10 + net->my_host_rank();
    net::PrefixSumForPowersOfTwo(*net, local_value);
    ASSERT_EQ(
        (net->my_host_rank() + 1) * 10 +
        net->my_host_rank() * (net->my_host_rank() + 1) / 2, local_value);
}

// let group of p hosts perform an ReduceToRoot collective
static void TestReduceToRoot(net::Group* net) {
    size_t local_value = net->my_host_rank();
    ReduceToRoot(*net, local_value);
    if (net->my_host_rank() == 0)
        ASSERT_EQ(local_value, net->num_hosts() * (net->num_hosts() - 1) / 2);
}

#endif // !THRILL_TESTS_NET_GROUP_TEST_BASE_HEADER

/******************************************************************************/
