/*******************************************************************************
 * tests/communication/test_net_dispatcher.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include <thread>
#include <vector>
#include <string>
#include "c7a/communication/net-group.hpp"
#include "c7a/communication/flow_control_channel.hpp"

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
        net->SendMsg(i, "Hello " + std::to_string(net->MyRank())
                     + " -> " + std::to_string(i));
    }
    // receive the n-1 messages from clients in order
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;

        std::string msg;
        net->ReceiveFrom(i, &msg);
        sLOG << "Received from client" << i << "msg" << msg;

        ASSERT_EQ(msg, "Hello " + std::to_string(i)
                  + " -> " + std::to_string(net->MyRank()));
    }

    // *****************************************************************

    // send another message to all other clients except ourselves.
    for (size_t i = 0; i != net->Size(); ++i)
    {
        if (i == net->MyRank()) continue;
        net->SendMsg(i, "Hello " + std::to_string(net->MyRank())
                     + " -> " + std::to_string(i));
    }
    // receive the n-1 messages from clients in order
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
    // Construct a NetGroup of 6 workers which do nothing but terminate.
    NetGroup::ExecuteLocalMock(6, ThreadInitializeSendReceive);
}

// TEST(NetGroup, InitializeAndClose) {
//     auto endpoints = { ExecutionEndpoint(0, "127.0.0.1:1234") };
//     auto candidate = NetDispatcher(0, endpoints);
//     ASSERT_EQ(candidate.Initialize(), NET_SERVER_SUCCESS);
//     candidate.Close();
// }

// void TestNetDispatcher(NetDispatcher* candidate)
// {
//     ASSERT_EQ(candidate->Initialize(), NET_SERVER_SUCCESS);

//     if (candidate->localId == candidate->masterId) {
//         MasterFlowControlChannel channel(candidate);

//         std::vector<std::string> messages = channel.ReceiveFromWorkers();
//         for (int i = 1; i != 4; ++i) {
//             ASSERT_EQ(messages[i], "Hello Master");
//         }
//         channel.BroadcastToWorkers("Hello Worker");

//         candidate->Close();
//     }
//     else {
//         WorkerFlowControlChannel channel(candidate);

//         channel.SendToMaster("Hello Master");
//         ASSERT_EQ(channel.ReceiveFromMaster(), "Hello Worker");

//         candidate->Close();
//     }
// }

// TEST(DISABLED_NetDispatcher, InitializeMultipleCommunication) {
//      const int count = 4;

//     ExecutionEndpoints endpoints = {
//         ExecutionEndpoint(0, "127.0.0.1:1234"),
//         ExecutionEndpoint(1, "127.0.0.1:1235"),
//         ExecutionEndpoint(2, "127.0.0.1:1236"),
//         ExecutionEndpoint(3, "127.0.0.1:1237")
//     };
//     NetDispatcher* candidates[count];
//     for (int i = 0; i < count; i++) {
//         candidates[i] = new NetDispatcher(i, endpoints);
//     }
//     std::thread threads[count];
//     for (int i = 0; i < count; i++) {
//         threads[i] = std::thread(TestNetDispatcher, candidates[i]);
//     }
//     for (int i = 0; i < count; i++) {
//         threads[i].join();
//     }
// }

/******************************************************************************/
