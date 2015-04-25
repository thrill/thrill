#include "gtest/gtest.h"
#include <thread>
#include <vector>
#include <string>
#include "c7a/communication/net_dispatcher.hpp"
#include "c7a/communication/flow_control_channel.hpp"

using namespace c7a::communication;
using namespace std;

TEST(NetDispatcher, InitializeAndClose) {
    auto endpoints = {ExecutionEndpoint::ParseEndpoint("127.0.0.1:1234", 0)};
    auto candidate = NetDispatcher(0, endpoints);
	ASSERT_EQ(candidate.Initialize(), NET_SERVER_SUCCESS);
    candidate.Close();
}

void TestNetDispatcher(NetDispatcher *candidate) {
	ASSERT_EQ(candidate->Initialize(), NET_SERVER_SUCCESS);


	if(candidate->localId == candidate->masterId) {
        MasterFlowControlChannel channel(candidate);

		vector<string> messages = channel.ReceiveFromWorkers();
		for(string message : messages) {
			ASSERT_EQ(message, "Hello Master");
		}
        channel.BroadcastToWorkers("Hello Worker");

        candidate->Close();
	} else {
        WorkerFlowControlChannel channel(candidate);
        
		channel.SendToMaster("Hello Master");
        ASSERT_EQ(channel.ReceiveFromMaster(), "Hello Worker");

        candidate->Close();
	}

};

TEST(NetDispatcher, InitializeMultipleCommunication) {

	const int count = 4;

    ExecutionEndpoints endpoints = {
    	ExecutionEndpoint::ParseEndpoint("127.0.0.1:1234", 0),
    	ExecutionEndpoint::ParseEndpoint("127.0.0.1:1235", 0),
    	ExecutionEndpoint::ParseEndpoint("127.0.0.1:1236", 0),
    	ExecutionEndpoint::ParseEndpoint("127.0.0.1:1237", 0)
    };
    NetDispatcher *candidates[count];
    for(int i = 0; i < count; i++) {
    	candidates[i] = new NetDispatcher(i, endpoints);
    }
    thread threads[count];
    for(int i = 0; i < count; i++) {
    	threads[i] = thread(TestNetDispatcher, candidates[i]);
    }
	for(int i = 0; i < count; i++) {
    	threads[i].join();
    }
}
