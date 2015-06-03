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

static void ThreadInitializeAsyncRead(Group* net) {
    // send a message to all other clients except ourselves.
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;
        net->connection(i).GetSocket().send(&i, sizeof(size_t));
    }

    size_t received = 0;
    Dispatcher dispatcher;

    Dispatcher::AsyncReadCallback callback =
        [net, &received](Connection& /* s */, const Buffer& buffer) {
            ASSERT_EQ(*(reinterpret_cast<const size_t*>(buffer.data())),
                      net->MyRank());
            received++;
        };

    // add async reads to net dispatcher
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;
        dispatcher.AsyncRead(net->connection(i), sizeof(size_t), callback);
    }

    while (received < net->Size() - 1) {
        dispatcher.Dispatch();
    }
}

static void ThreadInitializeBroadcastIntegral(Group* net) {

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

static void ThreadInitializeSendReceive(Group* net) {
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
        net->connection(i).SendString("Hello " + std::to_string(net->MyRank())
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

static void ThreadInitializeSendReceiveALot(Group* net) {
    for(int i = 0; i < 100; i++) {
        ThreadInitializeSendReceive(net);
    }
}

static void ThreadInitializeSendReceiveAsyncALot(Group* net) {
    for(int i = 0; i < 100; i++) {
        ThreadInitializeAsyncRead(net);
    }
}

static void RealGroupConstructAndCall(
    std::function<void(Group*)> thread_function) {
    // randomize base port number for test
    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(30000, 65000);
    const size_t port_base = distribution(generator);

    static const std::vector<Endpoint> endpoints = {
        Endpoint("127.0.0.1:" + std::to_string(port_base + 0)),
        Endpoint("127.0.0.1:" + std::to_string(port_base + 1)),
        Endpoint("127.0.0.1:" + std::to_string(port_base + 2)),
        Endpoint("127.0.0.1:" + std::to_string(port_base + 3)),
        Endpoint("127.0.0.1:" + std::to_string(port_base + 4)),
        Endpoint("127.0.0.1:" + std::to_string(port_base + 5))
    };

    sLOG1 << "Group test uses ports " << port_base << "-" << port_base + 5;

    static const int count = endpoints.size();

    std::vector<std::thread> threads(count);

    // lambda to construct Group and call user thread function.

    std::vector<Manager> groups(count);

    for (int i = 0; i < count; i++) {
        threads[i] = std::thread(
            [i, &thread_function, &groups]() {
                // construct Group i with endpoints
                groups[i].Initialize(i, endpoints);
                // run thread function
                thread_function(&groups[i].GetFlowGroup());
            });
    }

    for (int i = 0; i < count; i++) {
        threads[i].join();
    }
}

TEST(Group, RealInitializeAndClose) {
    // Construct a real Group of 6 workers which do nothing but terminate.
    RealGroupConstructAndCall([](Group*) { });
}

TEST(Group, RealInitializeSendReceive) {
    // Construct a real Group of 6 workers which execute the thread function
    // above which sends and receives a message from all neighbors.
    RealGroupConstructAndCall(ThreadInitializeSendReceive);
}

TEST(Group, RealInitializeSendReceiveALot) {
    // Construct a real Group of 6 workers which execute the thread function
    // above which sends and receives a message from all neighbors.
    RealGroupConstructAndCall(ThreadInitializeSendReceive);
}

TEST(Group, RealInitializeSendReceiveAsync) { //TODO(ej) test hangs from time to time
    // Construct a real Group of 6 workers which execute the thread function
    // which sends and receives asynchronous messages between all workers.
    RealGroupConstructAndCall(ThreadInitializeAsyncRead);
}

TEST(Group, RealInitializeSendReceiveAsyncALot) {
    // Construct a real Group of 6 workers which execute the thread function
    // above which sends and receives a message from all neighbors.
    RealGroupConstructAndCall(ThreadInitializeSendReceive);
}


TEST(Group, RealInitializeBroadcast) {
    // Construct a real Group of 6 workers which execute the thread function
    // above which sends and receives a message from all workers.
    RealGroupConstructAndCall(ThreadInitializeBroadcastIntegral);
}

TEST(Group, InitializeAndClose) {
    // Construct a Group of 6 workers which do nothing but terminate.
    Group::ExecuteLocalMock(6, [](Group*) { });
}

TEST(Manager, InitializeAndClose) {
    // Construct a Group of 6 workers which do nothing but terminate.
    Manager::ExecuteLocalMock(6, [](Group*) { }, [](Group*) { }, [](Group*) { });
}

TEST(Group, InitializeSendReceive) {
    // Construct a Group of 6 workers which execute the thread function
    // which sends and receives asynchronous messages between all workers.
    Group::ExecuteLocalMock(6, ThreadInitializeSendReceive);
}

TEST(Group, InitializeBroadcast) {
    // Construct a Group of 6 workers which execute the thread function
    // above which sends and receives a message from all workers.
    Group::ExecuteLocalMock(6, ThreadInitializeBroadcastIntegral);
}

/*
TEST(Group, TestPrefixSum) {
    for (size_t p = 2; p <= 8; p *= 2) {
        // Construct Group of p workers which perform a PrefixSum collective
        Group::ExecuteLocalMock(
            p, [](Group* net) {
                size_t local_value = 1;
                net->PrefixSum(local_value);
                ASSERT_EQ(local_value, net->MyRank() + 1);
            });
    }
}
*/

TEST(Group, TestAllReduce) {
    for (size_t p = 0; p <= 8; ++p) {
        // Construct Group of p workers which perform an AllReduce collective
        Group::ExecuteLocalMock(
            p, [](Group* net) {
                size_t local_value = net->MyRank();
                net->AllReduce(local_value);
                ASSERT_EQ(local_value, net->Size() * (net->Size() - 1) / 2);
            });
    }
}

TEST(Group, TestBroadcast) {
    for (size_t p = 0; p <= 8; ++p) {
        // Construct Group of p workers which perform an Broadcast collective
        Group::ExecuteLocalMock(
            p, [](Group* net) {
                size_t local_value;
                if (net->MyRank() == 0) local_value = 42;
                net->Broadcast(local_value);
                ASSERT_EQ(local_value, 42u);
            });
    }
}

TEST(Group, TestReduceToRoot) {
    for (size_t p = 0; p <= 8; ++p) {
        // Construct Group of p workers which perform an Broadcast collective
        Group::ExecuteLocalMock(
            p, [](Group* net) {
                size_t local_value = net->MyRank();
                net->ReduceToRoot(local_value);
                if (net->MyRank() == 0)
                    ASSERT_EQ(local_value, net->Size() * (net->Size() - 1) / 2);
            });
    }
}

/******************************************************************************/
