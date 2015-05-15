/*******************************************************************************
 * tests/data/test_data_manager.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "c7a/data/data_manager.hpp"
#include <c7a/net/net_dispatcher.hpp>
#include <c7a/net/lowlevel/socket.hpp>
#include <c7a/net/channel_multiplexer.hpp>
#include <thread>

using namespace c7a::data;
using namespace c7a::net;
using namespace c7a::net::lowlevel;

struct WorkerMock {
    WorkerMock()
        : dispatcher(),
          cmp(dispatcher),
          manager(cmp) { }

    void               Connect(std::shared_ptr<NetGroup> con) {
        cmp.Connect(con);
    }

    void               Run() {
        dispatcher.Dispatch();
    }

    NetDispatcher      dispatcher;
    ChannelMultiplexer cmp;
    DataManager        manager;
};

struct DataManagerChannelFixture : public::testing::Test {
    static const bool debug = true;
    DataManagerChannelFixture() {
        auto con1_2 = Socket::CreatePair();
        auto con1_3 = Socket::CreatePair();
        auto con2_3 = Socket::CreatePair();
        auto group1 = std::make_shared<NetGroup>(0, 3);
        auto group2 = std::make_shared<NetGroup>(1, 3);
        auto group3 = std::make_shared<NetGroup>(2, 3);
        auto net1_2 = NetConnection(std::get<0>(con1_2));
        auto net2_1 = NetConnection(std::get<1>(con1_2));
        auto net2_3 = NetConnection(std::get<0>(con2_3));
        auto net3_2 = NetConnection(std::get<1>(con2_3));
        auto net1_3 = NetConnection(std::get<0>(con1_3));
        auto net3_1 = NetConnection(std::get<1>(con1_3));
        group1->SetConnection(1, net1_2);
        group1->SetConnection(2, net1_3);
        group2->SetConnection(0, net2_1);
        group2->SetConnection(2, net2_3);
        group3->SetConnection(0, net3_1);
        group3->SetConnection(1, net3_2);

        worker1.Connect(group1);
        worker2.Connect(group2);
        worker3.Connect(group3);
    }

    void RunAll() {
        sLOG << "starting three worker mocks";
        auto t1 = std::thread(&WorkerMock::Run, &worker1);
        auto t2 = std::thread(&WorkerMock::Run, &worker2);
        auto t3 = std::thread(&WorkerMock::Run, &worker3);
        t1.join();
        t2.join();
        t3.join();
    }

    WorkerMock worker1;
    WorkerMock worker2;
    WorkerMock worker3;
};

TEST_F(DataManagerChannelFixture, EmptyChannels) {
    auto channel_id = worker1.manager.AllocateNetworkChannel();
    auto emitters = worker1.manager.GetNetworkEmitters<int>(channel_id);
}

/******************************************************************************/
