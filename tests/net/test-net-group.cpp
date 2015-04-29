/*******************************************************************************
 * tests/net/test-net-group.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/net/net-group.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <gtest/gtest.h>

#include <thread>
#include <vector>
#include <string>

using namespace c7a;

TEST(NetGroup, InitializeAndClose) {
    // Construct a NetGroup of 6 workers which do nothing but terminate.
    NetGroup::ExecuteLocalMock(6, [](NetGroup*) { });
}

static void ThreadInitializeSendReceive(NetGroup* net)
{
    static const bool debug = false;

    // send a message to all other clients except ourselves.
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;
        net->Connection(i).SendString("Hello " + std::to_string(net->MyRank())
                                      + " -> " + std::to_string(i));
    }
    // receive the n-1 messages from clients in order
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;

        std::string msg;
        net->Connection(i).ReceiveString(&msg);
        sLOG << "Received from client" << i << "msg" << msg;

        ASSERT_EQ(msg, "Hello " + std::to_string(i)
                  + " -> " + std::to_string(net->MyRank()));
    }

    // *****************************************************************

    // send another message to all other clients except ourselves.
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

TEST(NetGroup, InitializeSendReceive) {
    // Construct a NetGroup of 6 workers which execute the thread function above
    NetGroup::ExecuteLocalMock(6, ThreadInitializeSendReceive);
}

static void RealNetGroupConstructAndCall(
    std::function<void(NetGroup*)> thread_function)
{
    static const std::vector<NetEndpoint> endpoints = {
        NetEndpoint("127.0.0.1:11234"),
        NetEndpoint("127.0.0.1:11235"),
        NetEndpoint("127.0.0.1:11236"),
        NetEndpoint("127.0.0.1:11237"),
        NetEndpoint("127.0.0.1:11238"),
        NetEndpoint("127.0.0.1:11239")
    };

    static const int count = endpoints.size();

    std::vector<std::thread> threads(count);

    // lambda to construct NetGroup and call user thread function.

    for (int i = 0; i < count; i++) {
        threads[i] = std::thread(
            [i, &thread_function]() {
                // construct NetGroup i with endpoints
                NetGroup group(i, endpoints);
                // run thread function
                thread_function(&group);
                // TODO(tb): sleep here because otherwise connection may get
                // closed in ReceiveStringFromAny which causes an error.
                sleep(1);
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

TEST(NetGroup, TestAllReduce) {
    // Construct a NetGroup of 8 workers which perform an AllReduce collective
    NetGroup::ExecuteLocalMock(
        8, [](NetGroup* net) {
            size_t local_value = net->MyRank();
            net->AllReduce(local_value);
            ASSERT_EQ(local_value, net->Size() * (net->Size() - 1) / 2);
        });
}

/******************************************************************************/
