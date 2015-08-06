/*******************************************************************************
 * tests/net/group_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/net/collective_communication.hpp>
#include <c7a/net/dispatcher.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/group.hpp>
#include <c7a/net/manager.hpp>
#include <gtest/gtest.h>

#include <random>
#include <string>
#include <thread>
#include <vector>

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

static void ThreadInitializeSendCyclic(Group* net) {

    size_t id = net->MyRank();

    if (id != 0) {
        size_t res;
        net->ReceiveFrom<size_t>(id - 1, &res);
        ASSERT_EQ(id - 1, res);
    }

    if (id != net->Size() - 1) {
        net->SendTo(id + 1, id);
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

static void RealGroupConstructAndCall(
    std::function<void(Group*)> thread_function) {
    // randomize base port number for test
    std::default_random_engine generator({ std::random_device()() });
    std::uniform_int_distribution<int> distribution(10000, 30000);
    const size_t port_base = distribution(generator);

    std::vector<Endpoint> endpoints = {
        Endpoint("127.0.0.1:" + std::to_string(port_base + 0)),
        Endpoint("127.0.0.1:" + std::to_string(port_base + 1)),
        Endpoint("127.0.0.1:" + std::to_string(port_base + 2)),
        Endpoint("127.0.0.1:" + std::to_string(port_base + 3)),
        Endpoint("127.0.0.1:" + std::to_string(port_base + 4)),
        Endpoint("127.0.0.1:" + std::to_string(port_base + 5))
    };

    sLOG1 << "Group test uses ports" << port_base << "-" << port_base + 5;

    const size_t count = endpoints.size();

    std::vector<std::thread> threads(count);

    // lambda to construct Group and call user thread function.

    std::vector<Manager> groups(count);

    for (size_t i = 0; i < count; i++) {
        threads[i] = std::thread(
            [i, &endpoints, thread_function, &groups]() {
                // construct Group i with endpoints
                groups[i].Initialize(i, endpoints);
                // run thread function
                thread_function(&groups[i].GetFlowGroup());
            });
    }

    for (size_t i = 0; i < count; i++) {
        threads[i].join();
    }
    for (size_t i = 0; i < count; i++) {
        groups[i].Close();
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
TEST(Group, RealSendCyclic) {
    RealGroupConstructAndCall(ThreadInitializeSendCyclic);
}

TEST(Group, InitializeAndClose) {
    // Construct a Group of 6 workers which do nothing but terminate.
    Group::ExecuteLocalMock(6, [](Group*) { });
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

TEST(Group, SendCyclic) {
    Group::ExecuteLocalMock(6, ThreadInitializeSendCyclic);
}

TEST(Group, TestPrefixSum) {
    for (size_t p = 1; p <= 8; ++p) {
        // Construct Group of p workers which perform a PrefixSum collective
        Group::ExecuteLocalMock(
            p, [](Group* net) {
                size_t local_value = 1;
                PrefixSum(*net, local_value);
                ASSERT_EQ(local_value, net->MyRank() + 1);
            });
    }
}

TEST(Group, TestAllReduce) {
    for (size_t p = 0; p <= 8; ++p) {
        // Construct Group of p workers which perform an AllReduce collective
        Group::ExecuteLocalMock(
            p, [](Group* net) {
                size_t local_value = net->MyRank();
                AllReduce(*net, local_value);
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
                Broadcast(*net, local_value);
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
                ReduceToRoot(*net, local_value);
                if (net->MyRank() == 0)
                    ASSERT_EQ(local_value, net->Size() * (net->Size() - 1) / 2);
            });
    }
}

TEST(Group, TestBarrier) {
    static const bool debug = false;
    std::mutex sync_mtx;            // Synchronisation mutex for the barrier
    std::mutex local_mtx;           // Mutex for writing to the results array
    std::condition_variable cv;     // Condition variable for the barrier

    for (int p = 0; p <= 8; ++p) {
        int workers = p;
        int workers_copy = workers; // Will be decremented by the barrier function

        std::vector<char> result(2 * workers);
        int k = 0;                  // The counter for the result array
        sLOG << "I'm in test" << workers;

        Group::ExecuteLocalMock(
            workers, [&](Group* net) {
                local_mtx.lock();
                result[k++] = 'B'; // B stands for 'Before barrier'
                local_mtx.unlock();

                sLOG << "Before Barrier, worker" << net->MyRank();

                ThreadBarrier(sync_mtx, cv, workers_copy);

                local_mtx.lock();
                result[k++] = 'A'; // A stands for 'After barrier'
                local_mtx.unlock();

                sLOG << "After Barrier, worker" << net->MyRank();
            });
        for (int i = 0; i < workers; ++i) {
            sLOG << "Checking position" << i;
            ASSERT_EQ(result[i], 'B');
        }
        for (int i = workers; i < 2 * workers; ++i) {
            sLOG << "Checking position" << i;
            ASSERT_EQ(result[i], 'A');
        }
    }
}

/******************************************************************************/
