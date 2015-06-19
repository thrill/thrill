/*******************************************************************************
 * tests/data/manager_channels_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "c7a/data/manager.hpp"
#include <c7a/net/dispatcher.hpp>
#include <c7a/net/lowlevel/socket.hpp>
#include <c7a/net/channel_multiplexer.hpp>
#include <thread>

using namespace c7a::data;
using namespace c7a::net;
using namespace c7a::net::lowlevel;
using namespace std::literals; //for nicer sleep_for

struct DataManagerChannelFixture : public::testing::Test {
    DataManagerChannelFixture()
        : dispatcher(),
          worker0(dispatcher),
          worker1(dispatcher),
          worker2(dispatcher) {
    }

    using WorkerThread = std::function<void(data::Manager)>
    void FunctionSelect(NetGroup* group, WorkerThread f1, WorkerThread f2, WorkerThread f3) {
        data::Manager manager(dispatcher);
        manager.connect(group);
        switch(group->MyRank()) {
            case 0: f1(manager); break;
            case 1: f2(manager); break;
            case 3: f3(manager); break;
        }
    }

    void Execute(WorkerThread f1, WorkerThread f2, WorkerThread f3) {
        Group::ExecuteLocalMock(3, std::bind(std::placeholders::_1, f1, f2, f3));
    }

    template <class T>
    std::vector<T> ReadIterator(Iterator<T>& it) {
        std::vector<T> result;
        while (it.HasNext())
            result.push_back(it.Next());
        return result;
    }

    template <class T>
    bool VectorCompare(std::vector<T> a, std::vector<T> b) {
        if (a.size() != b.size())
            return false;
        for (auto& x : a) {
            if (std::find(b.begin(), b.end(), x) == b.end())
                return false;
        }
        return true;
    }

    ~DataManagerChannelFixture() {
        run = false;
        master.detach();
    }

    static const bool debug = true;
    DispatcherThread  dispatcher;
    WorkerMock        worker0;
    WorkerMock        worker1;
    WorkerMock        worker2;
};

TEST_F(DataManagerChannelFixture, EmptyChannels_GetIteratorDoesNotThrow) {
    auto channel_id = AllocateChannel();
    auto emitters = worker0.manager.GetNetworkEmitters<int>(channel_id);
    emitters[1].Close();

    //Worker 1 closed channel 0 on worker 2
    //Worker2 never allocated the channel id
    ASSERT_NO_THROW(worker1.manager.GetIterator<int>(channel_id));
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

    auto it = worker0.manager.GetIterator<int>(channel_id);
    ASSERT_TRUE(it.IsClosed());
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_IsNotClosedIfPartialClosed) {
    auto channel_id = AllocateChannel();
    auto emitter0 = worker0.manager.GetNetworkEmitters<int>(channel_id);
    auto emitter2 = worker2.manager.GetNetworkEmitters<int>(channel_id);

    //close incoming stream on worker 0
    emitter0[0].Close();
    emitter2[0].Close();

    auto it = worker0.manager.GetIterator<int>(channel_id);
    ASSERT_FALSE(it.IsClosed());
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_HasNextFalseWhenNotFlushed) {
    auto channel_id = AllocateChannel();
    auto emitter2 = worker2.manager.GetNetworkEmitters<int>(channel_id);

    emitter2[0](1);

    auto it = worker0.manager.GetIterator<int>(channel_id);
    ASSERT_FALSE(it.HasNext());
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_HasNextWhenFlushed) {
    auto channel_id = AllocateChannel();
    auto emitter2 = worker2.manager.GetNetworkEmitters<int>(channel_id);

    emitter2[0](1);
    emitter2[0].Flush();

    auto it = worker0.manager.GetIterator<int>(channel_id);
    ASSERT_TRUE(it.HasNext());
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_ReadsDataFromOneRemoteWorkerAndHasNoNextAfterwards) {
    auto channel_id = AllocateChannel();
    auto emitter2 = worker2.manager.GetNetworkEmitters<int>(channel_id);

    emitter2[0](1);
    emitter2[0].Flush();

    auto it = worker0.manager.GetIterator<int>(channel_id);
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

    auto it = worker0.manager.GetIterator<int>(channel_id);
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

    auto it = worker0.manager.GetIterator<int>(channel_id);
    auto vals = ReadIterator(it);
    ASSERT_TRUE(VectorCompare({ 1, 2, 3, 4 }, vals));
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_ReadsDataFromTwoChannels) {
    auto channel_id1 = AllocateChannel();
    auto channel_id2 = AllocateChannel();
    auto emitter1_1 = worker1.manager.GetNetworkEmitters<int>(channel_id1);
    auto emitter2_1 = worker1.manager.GetNetworkEmitters<int>(channel_id2);

    //w1 ends data to w0 via two channels
    emitter1_1[0](1);
    emitter2_1[0](1);

    emitter1_1[0].Close();
    emitter2_1[0].Close();

    auto it = worker0.manager.GetIterator<int>(channel_id1);
    auto vals = ReadIterator(it);
    ASSERT_TRUE(VectorCompare({ 1 }, vals));
    auto it2 = worker0.manager.GetIterator<int>(channel_id2);
    auto vals2 = ReadIterator(it2);
    ASSERT_TRUE(VectorCompare({ 2 }, vals2));
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

    auto it0 = worker0.manager.GetIterator<int>(channel_id);
    auto it1 = worker1.manager.GetIterator<int>(channel_id);
    auto it2 = worker2.manager.GetIterator<int>(channel_id);
    auto vals0 = ReadIterator(it0);
    auto vals1 = ReadIterator(it1);
    auto vals2 = ReadIterator(it2);
    ASSERT_TRUE(VectorCompare({ 1, 4 }, vals0));
    ASSERT_TRUE(VectorCompare({ 2 }, vals1));
    ASSERT_TRUE(VectorCompare({ 3 }, vals2));
}

/******************************************************************************/
