/*******************************************************************************
 * tests/net/group_test_base.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_TESTS_NET_GROUP_TEST_BASE_HEADER
#define THRILL_TESTS_NET_GROUP_TEST_BASE_HEADER

#include <thrill/common/math.hpp>
#include <thrill/net/collective.hpp>
#include <thrill/net/group.hpp>

#include <functional>
#include <future>
#include <string>
#include <tuple>
#include <vector>

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
    static constexpr bool debug = false;

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
    static constexpr bool debug = false;

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

/******************************************************************************/
// Collective Tests

//! let group of p hosts perform a PrefixSum collective
static void TestPrefixSumHypercube(net::Group* net) {
    // only for powers of two

// if (net->num_hosts() != common::RoundUpToPowerOfTwo(net->num_hosts()))
//    return;

    size_t local_value = 10 + net->my_host_rank();
    net::collective::PrefixSumHypercube(*net, local_value);
    ASSERT_EQ(
        (net->my_host_rank() + 1) * 10 +
        net->my_host_rank() * (net->my_host_rank() + 1) / 2, local_value);
}

//! let group of p hosts perform a PrefixSum collective on std::string
static void TestPrefixSumHypercubeString(net::Group* net) {
    // only for powers of two

    static constexpr bool debug = false;

    if (net->num_hosts() != common::RoundUpToPowerOfTwo(net->num_hosts()))
        return;

    const std::string result = "abcdefghijklmnopqrstuvwxyz";

    std::string local_value = result.substr(net->my_host_rank(), 1);
    net::collective::PrefixSumHypercube(*net, local_value);
    sLOG << "rank" << net->my_host_rank() << "hosts" << net->num_hosts()
         << "value" << local_value;
    ASSERT_EQ(result.substr(0, net->my_host_rank() + 1), local_value);
}

//! let group of p hosts perform a PrefixSum collective on std::string
static void TestPrefixSum(net::Group* net) {
    static constexpr bool debug = false;

    const std::string result = "abcdefghijklmnopqrstuvwxyz";

    {
        std::string local_value = result.substr(net->my_host_rank(), 1);
        net::collective::PrefixSum(*net, local_value, std::plus<std::string>(), true);
        sLOG << "rank" << net->my_host_rank() << "hosts" << net->num_hosts()
             << "value" << local_value;
        ASSERT_EQ(result.substr(0, net->my_host_rank() + 1), local_value);
    }
    {
        std::string local_value = result.substr(net->my_host_rank(), 1);
        net::collective::PrefixSum(*net, local_value, std::plus<std::string>(), false);
        sLOG << "rank" << net->my_host_rank() << "hosts" << net->num_hosts()
             << "value" << local_value;
        ASSERT_EQ(result.substr(0, net->my_host_rank()), local_value);
    }
}

//! construct group of p workers which perform an Broadcast collective
static void TestBroadcast(net::Group* net) {
    for (size_t origin = 0; origin < net->num_hosts(); ++origin) {
        size_t local_value = net->my_host_rank() == origin ? 42 : 0;
        net::collective::Broadcast(*net, local_value, origin);
        ASSERT_EQ(42u, local_value);
        // repeat with a different value.
        local_value = net->my_host_rank() == origin ? 6 * 9 : 0;
        net::collective::BroadcastBinomialTree(*net, local_value, origin);
        ASSERT_EQ(6 * 9u, local_value);
        // check trivial broadcast
        local_value = net->my_host_rank() == origin ? 5 : 0;
        net::collective::BroadcastTrivial(*net, local_value, origin);
        ASSERT_EQ(5u, local_value);
    }
}

// let group of p hosts perform an Reduce collective
static void TestReduce(net::Group* net) {
    size_t local_value = net->my_host_rank();
    net::collective::Reduce(*net, local_value);
    if (net->my_host_rank() == 0)
        ASSERT_EQ(local_value, net->num_hosts() * (net->num_hosts() - 1) / 2);
}

//! let group of p hosts perform a Reduce collective on std::string
static void TestReduceString(net::Group* net) {
    const std::string result = "abcdefghijklmnopqrstuvwxyz";
    std::string local_value = result.substr(net->my_host_rank(), 1);
    net::collective::Reduce(*net, local_value);
    if (net->my_host_rank() == 0)
        ASSERT_EQ(result.substr(0, net->num_hosts()), local_value);
}

//! let group of p hosts perform a AllReduce collective on std::string
static void TestAllReduceString(net::Group* net) {
    const std::string result = "abcdefghijklmnopqrstuvwxyz";
    std::string local_value = result.substr(net->my_host_rank(), 1);
    net::collective::AllReduce(*net, local_value);
    ASSERT_EQ(result.substr(0, net->num_hosts()), local_value);
}

//! let group of p hosts perform a AllReduce collective on std::string
static void TestAllReduceHypercubeString(net::Group* net) {
    if (net->num_hosts() != common::RoundUpToPowerOfTwo(net->num_hosts()))
        return;

    const std::string result = "abcdefghijklmnopqrstuvwxyz";
    std::string local_value = result.substr(net->my_host_rank(), 1);
    net::collective::AllReduceHypercube(*net, local_value);
    ASSERT_EQ(result.substr(0, net->num_hosts()), local_value);
}

/******************************************************************************/
// Dispatcher Tests

//! sends and receives asynchronous messages between all workers.
static void TestDispatcherSyncSendAsyncRead(net::Group* net) {
    // send a message to all other clients except ourselves.
    for (size_t i = 0; i < net->num_hosts(); ++i)
    {
        if (i == net->my_host_rank()) continue;
        net->connection(i).SyncSend(&i, sizeof(size_t));
    }

    size_t received = 0;
    mem::Manager mem_manager(nullptr, "Dispatcher");
    mem::unique_ptr<net::Dispatcher>
    dispatcher = net->ConstructDispatcher(mem_manager);

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
        dispatcher->AsyncRead(net->connection(i), sizeof(size_t), callback);
    }

    while (received < net->num_hosts() - 1) {
        dispatcher->Dispatch();
    }
}

/******************************************************************************/
// DispatcherThread tests

//! sleep for a new ticks until the dispatcher thread reaches select().
static void TestDispatcherLaunchAndTerminate(net::Group* net) {
    mem::Manager mem_manager_(nullptr, "DispatcherTest");
    net::DispatcherThread disp(mem_manager_, *net, "dispatcher");

    // sleep for a new ticks until the dispatcher thread reaches select().
    std::this_thread::sleep_for(std::chrono::microseconds(1));
}

//! use DispatcherThread to send and receive messages asynchronously.
//! this test produces a data race condition, which is probably a problem of
//! std::future
void DisabledTestDispatcherAsyncWriteAndReadIntoStdFuture(net::Group* net) {
    static constexpr bool debug = false;

    mem::Manager mem_manager_(nullptr, "DispatcherTest");

    net::DispatcherThread disp(mem_manager_, *net, "dispatcher");

    // send a message to all other clients except ourselves.
    for (size_t i = 0; i < net->num_hosts(); ++i) {
        if (i == net->my_host_rank()) continue;
        disp.AsyncWriteCopy(net->connection(i),
                            "Hello " + std::to_string(i % 10));
        sLOG << "I just sent Hello to" << i;
    }

    // issue async callbacks for getting messages from all other clients.

    std::vector<std::promise<net::Buffer> > results(net->num_hosts());

    for (size_t i = 0; i < net->num_hosts(); ++i) {
        if (i == net->my_host_rank()) continue;

        disp.AsyncRead(
            net->connection(i), 7,
            [i, &results](net::Connection&, net::Buffer&& b) -> void {
                sLOG << "Got Hello in callback from" << i;
                results[i].set_value(std::move(b));
            });
    }

    // wait for futures from all clients
    for (size_t i = 0; i < net->num_hosts(); ++i) {
        if (i == net->my_host_rank()) continue;

        std::future<net::Buffer> f = results[i].get_future();
        f.wait();
        net::Buffer b = f.get();

        sLOG << "Waiter got packet:" << b.ToString();
        ASSERT_EQ("Hello " + std::to_string(net->my_host_rank() % 10),
                  b.ToString());
    }
}

#endif // !THRILL_TESTS_NET_GROUP_TEST_BASE_HEADER

/******************************************************************************/
