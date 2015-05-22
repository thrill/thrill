/*******************************************************************************
 * tests/data/test_data_manager_channels.cpp
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
using namespace std::literals; //for nicer sleep_for

struct WorkerMock {
    WorkerMock(NetDispatcher& dispatcher)
        : cmp(dispatcher),
          manager(cmp) { }

    void               Connect(std::shared_ptr<NetGroup> con)
    {
        cmp.Connect(con);
    }

    ~WorkerMock()
    {
        cmp.Close();
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
          worker2(dispatcher)
    {
        auto con0_1 = Socket::CreatePair();
        auto con0_2 = Socket::CreatePair();
        auto con1_2 = Socket::CreatePair();
        auto group0 = std::make_shared<NetGroup>(0, 3);
        auto group1 = std::make_shared<NetGroup>(1, 3);
        auto group2 = std::make_shared<NetGroup>(2, 3);
        auto net0_1 = NetConnection(std::get<0>(con0_1), 0, 1);
        auto net1_0 = NetConnection(std::get<1>(con0_1), 1, 0);
        auto net1_2 = NetConnection(std::get<0>(con1_2), 1, 2);
        auto net2_1 = NetConnection(std::get<1>(con1_2), 2, 1);
        auto net0_2 = NetConnection(std::get<0>(con0_2), 0, 2);
        auto net2_0 = NetConnection(std::get<1>(con0_2), 2, 0);
        group0->AssignConnection(net0_1);
        group0->AssignConnection(net0_2);
        group1->AssignConnection(net1_0);
        group1->AssignConnection(net1_2);
        group2->AssignConnection(net2_0);
        group2->AssignConnection(net2_1);

        worker0.Connect(group0);
        worker1.Connect(group1);
        worker2.Connect(group2);
    }

    void RunDispatcherLoop()
    {
        master = std::thread([&]() {
                                 sLOG << "Spinning up that dispatcher biest!";
                                 run = true;
                                 while (run)
                                     dispatcher.Dispatch();
                                 sLOG << "Something is wrong! Dispatcher returned";
                             });
        //required because DetachAll, must happen *after* threads ran
        std::this_thread::sleep_for(100ms);
    }

    size_t AllocateChannel()
    {
        //all managers need to allocate the same id
        auto channel_id = worker0.manager.AllocateNetworkChannel();
        channel_id = worker1.manager.AllocateNetworkChannel();
        channel_id = worker2.manager.AllocateNetworkChannel();
        return channel_id;
    }

    template <class T>
    std::vector<T> ReadIterator(BlockIterator<T>& it)
    {
        std::vector<T> result;
        while (it.HasNext())
            result.push_back(it.Next());
        return result;
    }

    template <class T>
    bool VectorCompare(std::vector<T> a, std::vector<T> b)
    {
        if (a.size() != b.size())
            return false;
        for (auto& x : a) {
            if (std::find(b.begin(), b.end(), x) == b.end())
                return false;
        }
        return true;
    }

    ~DataManagerChannelFixture()
    {
        run = false;
        master.detach();
    }

    static const bool debug = true;
    bool              run;
    NetDispatcher     dispatcher;
    std::thread       master;
    WorkerMock        worker0;
    WorkerMock        worker1;
    WorkerMock        worker2;
};

TEST_F(DataManagerChannelFixture, EmptyChannels_GetRemoteBlocksDoesNotThrow) {
    auto channel_id = AllocateChannel();
    auto emitters = worker0.manager.GetNetworkEmitters<int>(channel_id);
    emitters[1].Close();

    RunDispatcherLoop();

    //Worker 1 closed channel 0 on worker 2
    //Worker2 never allocated the channel id
    ASSERT_NO_THROW(worker1.manager.GetRemoteBlocks<int>(channel_id));
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_IsClosed) {
    auto channel_id = AllocateChannel();
    auto emitter0 = worker0.manager.GetNetworkEmitters<int>(channel_id);
    auto emitter1 = worker1.manager.GetNetworkEmitters<int>(channel_id);
    auto emitter2 = worker2.manager.GetNetworkEmitters<int>(channel_id);

    //close incoming stream on worker 0
    emitter0[0].Close();
    emitter1[0].Close();
    emitter2[0].Close();

    RunDispatcherLoop();
    auto it = worker0.manager.GetRemoteBlocks<int>(channel_id);
    ASSERT_TRUE(it.IsClosed());
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_IsNotClosedIfPartialClosed) {
    auto channel_id = AllocateChannel();
    auto emitter0 = worker0.manager.GetNetworkEmitters<int>(channel_id);
    auto emitter2 = worker2.manager.GetNetworkEmitters<int>(channel_id);

    //close incoming stream on worker 0
    emitter0[0].Close();
    emitter2[0].Close();

    RunDispatcherLoop();
    auto it = worker0.manager.GetRemoteBlocks<int>(channel_id);
    ASSERT_FALSE(it.IsClosed());
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_HasNextFalseWhenNotFlushed) {
    auto channel_id = AllocateChannel();
    auto emitter2 = worker2.manager.GetNetworkEmitters<int>(channel_id);

    emitter2[0](1);

    RunDispatcherLoop();
    auto it = worker0.manager.GetRemoteBlocks<int>(channel_id);
    ASSERT_FALSE(it.HasNext());
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_HasNextWhenFlushed) {
    auto channel_id = AllocateChannel();
    auto emitter2 = worker2.manager.GetNetworkEmitters<int>(channel_id);

    emitter2[0](1);
    emitter2[0].Flush();

    RunDispatcherLoop();
    auto it = worker0.manager.GetRemoteBlocks<int>(channel_id);
    ASSERT_TRUE(it.HasNext());
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_ReadsDataFromOneRemoteWorkerAndHasNoNextAfterwards) {
    auto channel_id = AllocateChannel();
    auto emitter2 = worker2.manager.GetNetworkEmitters<int>(channel_id);

    emitter2[0](1);
    emitter2[0].Flush();

    RunDispatcherLoop();
    auto it = worker0.manager.GetRemoteBlocks<int>(channel_id);
    ASSERT_EQ(1, it.Next());
    ASSERT_FALSE(it.HasNext());
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_ReadsDataFromOneRemoteWorkerMultipleFlushes) {
    auto channel_id = AllocateChannel();
    auto emitter2 = worker2.manager.GetNetworkEmitters<int>(channel_id);

    emitter2[0](1);
    emitter2[0].Flush();
    emitter2[0](2);
    emitter2[0](3);
    emitter2[0].Flush();
    emitter2[0](4);
    emitter2[0](5);
    emitter2[0](6);
    emitter2[0].Flush();

    RunDispatcherLoop();
    auto it = worker0.manager.GetRemoteBlocks<int>(channel_id);
    ASSERT_EQ(1, it.Next());
    ASSERT_TRUE(it.HasNext());
    ASSERT_EQ(2, it.Next());
    ASSERT_EQ(3, it.Next());
    ASSERT_TRUE(it.HasNext());
    ASSERT_EQ(4, it.Next());
    ASSERT_EQ(5, it.Next());
    ASSERT_EQ(6, it.Next());
    ASSERT_FALSE(it.HasNext());
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_ReadsDataFromMultipleWorkers) {
    auto channel_id = AllocateChannel();
    auto emitter1 = worker1.manager.GetNetworkEmitters<int>(channel_id);
    auto emitter2 = worker2.manager.GetNetworkEmitters<int>(channel_id);

    emitter1[0](2);
    emitter1[0](3);
    emitter2[0](1);
    emitter2[0](4);
    emitter1[0].Flush();
    emitter2[0].Close();

    RunDispatcherLoop();
    auto it = worker0.manager.GetRemoteBlocks<int>(channel_id);
    auto vals = ReadIterator(it);
    ASSERT_TRUE(VectorCompare({ 1, 2, 3, 4 }, vals));
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_SendsDataToMultipleWorkers) {
    auto channel_id = AllocateChannel();
    auto emitter1 = worker1.manager.GetNetworkEmitters<int>(channel_id);

    emitter1[0](1);
    emitter1[1](2);
    emitter1[2](3);
    emitter1[0](4);
    emitter1[0].Flush();
    emitter1[1].Flush();
    emitter1[2].Close();

    RunDispatcherLoop();
    auto it0 = worker0.manager.GetRemoteBlocks<int>(channel_id);
    auto it1 = worker1.manager.GetRemoteBlocks<int>(channel_id);
    auto it2 = worker2.manager.GetRemoteBlocks<int>(channel_id);
    auto vals0 = ReadIterator(it0);
    auto vals1 = ReadIterator(it1);
    auto vals2 = ReadIterator(it2);
    ASSERT_TRUE(VectorCompare({ 1, 4 }, vals0));
    ASSERT_TRUE(VectorCompare({ 2 }, vals1));
    ASSERT_TRUE(VectorCompare({ 3 }, vals2));
}

/******************************************************************************/
