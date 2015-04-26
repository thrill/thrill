/*******************************************************************************
 * tests/communication/test_net_dispatcher.cpp
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
#include "c7a/communication/net_dispatcher.hpp"
#include "c7a/communication/flow_control_channel.hpp"

using namespace c7a;
using namespace std;

TEST(NetDispatcher, InitializeAndClose) {
    auto endpoints = { ExecutionEndpoint::ParseEndpoint("127.0.0.1:1234", 0) };
    auto candidate = NetDispatcher(0, endpoints);
    ASSERT_EQ(candidate.Initialize(), NET_SERVER_SUCCESS);
    candidate.Close();
}

void TestNetDispatcher(NetDispatcher* candidate)
{
    ASSERT_EQ(candidate->Initialize(), NET_SERVER_SUCCESS);

    if (candidate->localId == candidate->masterId) {
        MasterFlowControlChannel channel(candidate);

        vector<string> messages = channel.ReceiveFromWorkers();
        for (int i = 1; i != 4; ++i) {
            ASSERT_EQ(messages[i], "Hello Master");
        }
        channel.BroadcastToWorkers("Hello Worker");

        candidate->Close();
    }
    else {
        WorkerFlowControlChannel channel(candidate);

        channel.SendToMaster("Hello Master");
        ASSERT_EQ(channel.ReceiveFromMaster(), "Hello Worker");

        candidate->Close();
    }
}

TEST(NetDispatcher, InitializeMultipleCommunication) {
    const int count = 4;

    ExecutionEndpoints endpoints = {
        ExecutionEndpoint::ParseEndpoint("127.0.0.1:1234", 0),
        ExecutionEndpoint::ParseEndpoint("127.0.0.1:1235", 1),
        ExecutionEndpoint::ParseEndpoint("127.0.0.1:1236", 2),
        ExecutionEndpoint::ParseEndpoint("127.0.0.1:1237", 3)
    };
    NetDispatcher* candidates[count];
    for (int i = 0; i < count; i++) {
        candidates[i] = new NetDispatcher(i, endpoints);
    }
    thread threads[count];
    for (int i = 0; i < count; i++) {
        threads[i] = thread(TestNetDispatcher, candidates[i]);
    }
    for (int i = 0; i < count; i++) {
        threads[i].join();
    }
}

/******************************************************************************/
