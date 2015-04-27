/*******************************************************************************
 * tests/net/test-net-group.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include <thread>
#include <vector>
#include <string>
#include <c7a/net/net-group.hpp>
#include <c7a/net/flow_control_channel.hpp>

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
        net->ReceiveFromAny(&from, &msg);
        sLOG << "Received from client" << i << "msg" << msg;

        ASSERT_EQ(msg, "Hello " + std::to_string(from)
                  + " -> " + std::to_string(net->MyRank()));
    }
}

TEST(NetGroup, InitializeSendReceive) {
    // Construct a NetGroup of 6 workers which execute the thread function above
    NetGroup::ExecuteLocalMock(6, ThreadInitializeSendReceive);
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
