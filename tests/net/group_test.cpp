/*******************************************************************************
 * tests/net/group_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/net/net_group.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/net_dispatcher.hpp>
#include <c7a/net/net_manager.hpp>
#include <gtest/gtest.h>

#include <thread>
#include <vector>
#include <string>
#include <random>

using namespace c7a::net;

static void ThreadInitializeAsyncRead(NetGroup* net) {
    // send a message to all other clients except ourselves.
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;
        net->Connection(i).GetSocket().send(&i, sizeof(size_t));
    }

    size_t received = 0;
    NetDispatcher dispatcher;

    NetDispatcher::AsyncReadCallback callback =
        [net, &received](NetConnection& /* s */, const Buffer& buffer) {
            ASSERT_EQ(*(reinterpret_cast<const size_t*>(buffer.data())),
                      net->MyRank());
            received++;
        };

    // add async reads to net dispatcher
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;
        dispatcher.AsyncRead(net->Connection(i), sizeof(size_t), callback);
    }

    while (received < net->Size() - 1) {
        dispatcher.Dispatch();
    }
}

static void ThreadInitializeBroadcastIntegral(NetGroup* net) {

    static const bool debug = false;

    //Broadcast our ID to everyone
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;
        net->SendTo(i, net->MyRank());
    }

    //Receive the id from everyone. Make sure that the id is correct.
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;

        size_t val;
        ClientId id;

        net->ReceiveFromAny<size_t>(&id, &val);

        LOG << "Received " << val << " from " << id;

        ASSERT_EQ((int)id, (int)val);
    }
}

static void ThreadInitializeSendReceive(NetGroup* net) {
    static const bool debug = false;

    // send a message to all other clients except ourselves.
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;
        net->SendStringTo(i, "Hello " + std::to_string(net->MyRank())
                          + " -> " + std::to_string(i));
    }
    // receive the n-1 messages from clients in order
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;

        std::string msg;
        net->ReceiveStringFrom(i, &msg);
        sLOG << "Received from client" << i << "msg" << msg;

        ASSERT_EQ(msg, "Hello " + std::to_string(i)
                  + " -> " + std::to_string(net->MyRank()));
    }

    // *****************************************************************

    // send another message to all other clients except ourselves. Now with connection access.
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;
        net->Connection(i).SendString("Hello " + std::to_string(net->MyRank())
                                      + " -> " + std::to_string(i));
    }
    // receive the n-1 messages from clients in any order
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;

        ClientId from;
        std::string msg;
        net->ReceiveStringFromAny(&from, &msg);
        sLOG << "Received from client" << i << "msg" << msg;

        ASSERT_EQ(msg, "Hello " + std::to_string(from)
                  + " -> " + std::to_string(net->MyRank()));
    }
}

static void RealNetGroupConstructAndCall(
    std::function<void(NetGroup*)> thread_function) {
    // randomize base port number for test
    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(30000, 65000);
    const size_t port_base = distribution(generator);

    static const std::vector<NetEndpoint> endpoints = {
        NetEndpoint("127.0.0.1:" + std::to_string(port_base + 0)),
        NetEndpoint("127.0.0.1:" + std::to_string(port_base + 1)),
        NetEndpoint("127.0.0.1:" + std::to_string(port_base + 2)),
        NetEndpoint("127.0.0.1:" + std::to_string(port_base + 3)),
        NetEndpoint("127.0.0.1:" + std::to_string(port_base + 4)),
        NetEndpoint("127.0.0.1:" + std::to_string(port_base + 5))
    };

    sLOG1 << "NetGroup test uses ports " << port_base << "-" << port_base + 5;

    static const int count = endpoints.size();

    std::vector<std::thread> threads(count);

    // lambda to construct NetGroup and call user thread function.

    std::vector<NetManager> groups(count);

    for (int i = 0; i < count; i++) {
        threads[i] = std::thread(
            [i, &thread_function, &groups]() {
                // construct NetGroup i with endpoints
                groups[i].Initialize(i, endpoints);
                // run thread function
                thread_function(&groups[i].GetFlowNetGroup());
            });
    }

    for (int i = 0; i < count; i++) {
        threads[i].join();
    }
}

TEST(NetGroup, RealInitializeAndClose) {
    // Construct a real NetGroup of 6 workers which do nothing but terminate.
    RealNetGroupConstructAndCall([](NetGroup*) { });
}

TEST(NetGroup, RealInitializeSendReceive) {
    // Construct a real NetGroup of 6 workers which execute the thread function
    // above which sends and receives a message from all neighbors.
    RealNetGroupConstructAndCall(ThreadInitializeSendReceive);
}

TEST(NetGroup, DISABLED_RealInitializeSendReceiveAsync) { //TODO(ej) test hangs from time to time
    // Construct a real NetGroup of 6 workers which execute the thread function
    // which sends and receives asynchronous messages between all workers.
    RealNetGroupConstructAndCall(ThreadInitializeAsyncRead);
}

TEST(NetGroup, RealInitializeBroadcast) {
    // Construct a real NetGroup of 6 workers which execute the thread function
    // above which sends and receives a message from all workers.
    RealNetGroupConstructAndCall(ThreadInitializeBroadcastIntegral);
}

TEST(NetGroup, InitializeAndClose) {
    // Construct a NetGroup of 6 workers which do nothing but terminate.
    NetGroup::ExecuteLocalMock(6, [](NetGroup*) { });
}

TEST(NetManager, InitializeAndClose) {
    // Construct a NetGroup of 6 workers which do nothing but terminate.
    NetManager::ExecuteLocalMock(6, [](NetGroup*) { }, [](NetGroup*) { }, [](NetGroup*) { });
}

TEST(NetGroup, InitializeSendReceive) {
    // Construct a NetGroup of 6 workers which execute the thread function
    // which sends and receives asynchronous messages between all workers.
    NetGroup::ExecuteLocalMock(6, ThreadInitializeSendReceive);
}

TEST(NetGroup, InitializeBroadcast) {
    // Construct a NetGroup of 6 workers which execute the thread function
    // above which sends and receives a message from all workers.
    NetGroup::ExecuteLocalMock(6, ThreadInitializeBroadcastIntegral);
}

/*
TEST(NetGroup, TestPrefixSum) {
    for (size_t p = 2; p <= 8; p *= 2) {
        // Construct NetGroup of p workers which perform a PrefixSum collective
        NetGroup::ExecuteLocalMock(
            p, [](NetGroup* net) {
                size_t local_value = 1;
                net->PrefixSum(local_value);
                ASSERT_EQ(local_value, net->MyRank() + 1);
            });
    }
}
*/

TEST(NetGroup, TestAllReduce) {
    for (size_t p = 0; p <= 8; ++p) {
        // Construct NetGroup of p workers which perform an AllReduce collective
        NetGroup::ExecuteLocalMock(
            p, [](NetGroup* net) {
                size_t local_value = net->MyRank();
                net->AllReduce(local_value);
                ASSERT_EQ(local_value, net->Size() * (net->Size() - 1) / 2);
            });
    }
}

TEST(NetGroup, TestBroadcast) {
    for (size_t p = 0; p <= 8; ++p) {
        // Construct NetGroup of p workers which perform an Broadcast collective
        NetGroup::ExecuteLocalMock(
            p, [](NetGroup* net) {
                size_t local_value;
                if (net->MyRank() == 0) local_value = 42;
                net->Broadcast(local_value);
                ASSERT_EQ(local_value, 42u);
            });
    }
}

TEST(NetGroup, TestReduceToRoot) {
    for (size_t p = 0; p <= 8; ++p) {
        // Construct NetGroup of p workers which perform an Broadcast collective
        NetGroup::ExecuteLocalMock(
            p, [](NetGroup* net) {
                size_t local_value = net->MyRank();
                net->ReduceToRoot(local_value);
                if (net->MyRank() == 0)
                    ASSERT_EQ(local_value, net->Size() * (net->Size() - 1) / 2);
            });
    }
}

/******************************************************************************/
