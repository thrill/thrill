/*******************************************************************************
 * tests/net/group_test_base.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_TESTS_NET_GROUP_TEST_BASE_HEADER
#define THRILL_TESTS_NET_GROUP_TEST_BASE_HEADER

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
        net->SendStringTo(i, "Hello " + std::to_string(net->my_host_rank())
                          + " -> " + std::to_string(i));
    }
    // receive the n-1 messages from clients in order
    for (size_t i = 0; i != net->num_hosts(); ++i)
    {
        if (i == net->my_host_rank()) continue;

        std::string msg;
        net->ReceiveStringFrom(i, &msg);
        sLOG << "Received from client" << i << "msg" << msg;

        ASSERT_EQ(msg, "Hello " + std::to_string(i)
                  + " -> " + std::to_string(net->my_host_rank()));
    }
}

//! let group of p hosts perform a PrefixSum collective
static void TestPrefixSumForPowersOfTwo(net::Group* net) {
    size_t local_value = 1;
    net::PrefixSumForPowersOfTwo(*net, local_value);
    ASSERT_EQ(local_value, net->my_host_rank() + 1);
}

#endif // !THRILL_TESTS_NET_GROUP_TEST_BASE_HEADER

/******************************************************************************/
