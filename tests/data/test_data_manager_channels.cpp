/*******************************************************************************
 * tests/data/test_data_manager.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
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
using namespace std::literals;

struct WorkerMock {
    WorkerMock(NetDispatcher& dispatcher)
        : cmp(dispatcher),
          manager(cmp) { }

    void               Connect(std::shared_ptr<NetGroup> con) {
        cmp.Connect(con);
    }

    ChannelMultiplexer cmp;
    DataManager        manager;
    bool               run;
};

struct DataManagerChannelFixture : public::testing::Test {
    DataManagerChannelFixture()
        : dispatcher(),
          worker0(dispatcher),
          worker1(dispatcher),
          worker2(dispatcher) {
        auto con0_1 = Socket::CreatePair();
        auto con0_2 = Socket::CreatePair();
        auto con1_2 = Socket::CreatePair();
        auto group0 = std::make_shared<NetGroup>(0, 3);
        auto group1 = std::make_shared<NetGroup>(1, 3);
        auto group2 = std::make_shared<NetGroup>(2, 3);
        auto net0_1 = NetConnection(std::get<0>(con0_1));
        auto net1_0 = NetConnection(std::get<1>(con0_1));
        auto net1_2 = NetConnection(std::get<0>(con1_2));
        auto net2_1 = NetConnection(std::get<1>(con1_2));
        auto net0_2 = NetConnection(std::get<0>(con0_2));
        auto net2_0 = NetConnection(std::get<1>(con0_2));
        group0->SetConnection(1, net0_1);
        group0->SetConnection(2, net0_2);
        group1->SetConnection(0, net1_0);
        group1->SetConnection(2, net1_2);
        group2->SetConnection(0, net2_0);
        group2->SetConnection(1, net2_1);

        worker0.Connect(group0);
        worker1.Connect(group1);
        worker2.Connect(group2);
    }

    void RunDispatcherLoop() {
        master = std::thread([&]() {
                                 sLOG << "Spinning up that dispatcher biest!";
                                 dispatcher.Dispatch();
                                 sLOG << "Something is wrong! Dispatcher returned";
                             });
        //required because DetachAll, must happen *after* threads ran
        std::this_thread::sleep_for(50000ms);
    }

    ~DataManagerChannelFixture() {
        master.detach();
    }

    static const bool debug = true;
    NetDispatcher     dispatcher;
    std::thread       master;
    WorkerMock        worker0;
    WorkerMock        worker1;
    WorkerMock        worker2;
};

TEST_F(DataManagerChannelFixture, EmptyChannelsWithOutAlloc_GetRemoteBlocksDoesNotThrow) {
    auto channel_id = worker0.manager.AllocateNetworkChannel();
    auto emitters = worker0.manager.GetNetworkEmitters<int>(channel_id);
    emitters[1].Close();

    RunDispatcherLoop();

    //Worker 1 closed channel 0 on worker 2
    //Worker2 never allocated the channel id
    worker1.manager.GetRemoteBlocks<int>(channel_id);
}

TEST_F(DataManagerChannelFixture, DISABLED_GetNetworkBlocks_IsClosed) {
    auto channel_id0 = worker0.manager.AllocateNetworkChannel();
    auto channel_id1 = worker1.manager.AllocateNetworkChannel();
    auto channel_id2 = worker2.manager.AllocateNetworkChannel();
    auto emitters1 = worker0.manager.GetNetworkEmitters<int>(channel_id0);
    auto emitters2 = worker1.manager.GetNetworkEmitters<int>(channel_id1);
    auto emitters3 = worker2.manager.GetNetworkEmitters<int>(channel_id2);

    //close incoming stream on worker 0
    emitters1[0].Close();
    emitters2[0].Close();
    emitters3[0].Close();

    RunDispatcherLoop();
    auto it = worker0.manager.GetRemoteBlocks<int>(channel_id1);
    ASSERT_TRUE(it.IsClosed());
}

/******************************************************************************/
