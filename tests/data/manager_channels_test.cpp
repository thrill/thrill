/*******************************************************************************
 * tests/data/manager_channels_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "c7a/data/manager.hpp"
#include "c7a/common/logger.hpp"
#include <c7a/net/dispatcher.hpp>
#include <c7a/net/lowlevel/socket.hpp>
#include <c7a/net/channel_multiplexer.hpp>
#include <c7a/common/cyclic_barrier.hpp>
#include <thread>
#include <stdlib.h> //free

using namespace c7a::data;
using namespace c7a::common;
using namespace c7a::net;
using namespace c7a::net::lowlevel;
using namespace std::literals; //for nicer sleep_for

struct DataManagerChannelFixture : public::testing::Test {
    DataManagerChannelFixture()
        : dispatcher("dispatcher"), manager(dispatcher), single_group(0, 1) {
        manager.Connect(&single_group);
    }

    using WorkerThread = std::function<void(Manager&)>;
    void FunctionSelect(Group* group, WorkerThread f1, WorkerThread f2, WorkerThread f3 = [](Manager&) { }) {
        Manager manager(dispatcher);
        manager.Connect(group);
        switch (group->MyRank()) {
        case 0:
            ThreadDirectory.NameThisThread("t0");
            f1(manager);
            break;
        case 1:
            ThreadDirectory.NameThisThread("t1");
            f2(manager);
            break;
        case 2:
            ThreadDirectory.NameThisThread("t2");
            f3(manager);
            break;
        }
        barrier->Await();
    }

    void Execute(WorkerThread f1, WorkerThread f2, WorkerThread f3) {
        barrier = new Barrier(3);
        Group::ExecuteLocalMock(3,
                                [=](Group* g) {
                                    FunctionSelect(g, f1, f2, f3);
                                });
        free(barrier);
    }

    void Execute(WorkerThread f1, WorkerThread f2) {
        barrier = new Barrier(2);
        Group::ExecuteLocalMock(2,
                                [=](Group* g) {
                                    FunctionSelect(g, f1, f2);
                                });
        free(barrier);
    }

    void Execute(WorkerThread f1) {
        barrier = new Barrier(1);
        Group::ExecuteLocalMock(1,
                                [=](Group* g) {
                                    FunctionSelect(g, f1, [](Manager&) { });
                                });
        free(barrier);
    }

    template <class T>
    static std::vector<T> ReadIterator(Iterator<T>& it, bool wait_for_all = false) {
        sLOG << "reading iterator";
        std::vector<T> result;
        do {
            if (wait_for_all)
                it.WaitForAll();
            while (it.HasNext()) {
                auto element = it.Next();
                std::cout << "read '" << element << "'" << std::endl;
                result.push_back(element);
            }
        } while (!it.IsFinished() && wait_for_all);
        return result;
    }

    template <class T>
    static bool VectorCompare(std::vector<T> a, std::vector<T> b) {
        if (a.size() != b.size())
            return false;
        for (auto& x : a) {
            if (std::find(b.begin(), b.end(), x) == b.end())
                return false;
        }
        return true;
    }

    template <class T>
    static bool OrderedVectorCompare(std::vector<T> a, std::vector<T> b) {
        if (a.size() != b.size()) {
            std::cout << "vectors differ in size (" << a.size() << " vs. " << b.size() << ")" << std::endl;
            return false;
        }
        for (size_t i = 0; i < a.size(); i++)
            if (a[i] != b[i]) {
                std::cout << a[i] << " differs from " << b[i] << " @ " << i << std::endl;
                return false;
            }
        return true;
    }

    static const bool debug = true;
    DispatcherThread  dispatcher;
    Manager           manager;
    Group             single_group;
    Barrier           * barrier;
};

TEST_F(DataManagerChannelFixture, EmptyChannels_GetIteratorDoesNotThrow) {
    Barrier sync(2);
    auto w0 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto emitters = manager.GetNetworkEmitters<int>(channel_id);
                  emitters[1].Close();
                  emitters[0].Close();
                  std::this_thread::sleep_for(2ms);
                  sync.Await();
                  ASSERT_NO_THROW(manager.GetIterator<int>(channel_id));
              };

    auto w1 = [&sync](Manager& manager) {
                  sync.Await();
                  auto channel_id = manager.AllocateNetworkChannel();
                  ASSERT_NO_THROW(manager.GetIterator<int>(channel_id));
              };
    Execute(w0, w1);
}

TEST_F(DataManagerChannelFixture, Scatter_OneWorker) {
    auto w0 = [](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel(true);
                  auto src_id = manager.AllocateDIA();
                  auto emitter = manager.GetLocalEmitter<std::string>(src_id);
                  emitter("foo");
                  emitter("bar");
                  emitter.Flush();
                  emitter("breakfast is the most important meal of the day.");
                  emitter.Close();
                  manager.Scatter<std::string>(src_id, channel_id, { 3 });
                  auto it = manager.GetIterator<std::string>(channel_id);
                  ASSERT_TRUE(it.HasNext());
                  ASSERT_EQ(it.Next(), "foo");
                  ASSERT_EQ(it.Next(), "bar");
                  ASSERT_EQ(it.Next(), "breakfast is the most important meal of the day.");
                  ASSERT_TRUE(it.IsFinished());
              };

    Execute(w0);
}

TEST_F(DataManagerChannelFixture, Scatter_TwoWorkers_OnlyLocalCopy) {
    Barrier sync(2);
    auto w0 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel(true);
                  sync.Await();
                  auto src_id = manager.AllocateDIA();
                  auto emitter = manager.GetLocalEmitter<std::string>(src_id);
                  emitter("foo");
                  emitter("bar");
                  emitter.Close();
                  manager.Scatter<std::string>(src_id, channel_id, { 2, 2 });
                  auto it = manager.GetIterator<std::string>(channel_id);
                  auto vals = ReadIterator(it, true);
                  ASSERT_TRUE(OrderedVectorCompare({ "foo", "bar" }, vals));
              };
    auto w1 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel(true);
                  sync.Await();
                  auto src_id = manager.AllocateDIA();
                  auto emitter = manager.GetLocalEmitter<std::string>(src_id);
                  emitter("hello");
                  emitter("world");
                  emitter(".");
                  emitter.Close();
                  manager.Scatter<std::string>(src_id, channel_id, { 0, 3 });
                  auto it = manager.GetIterator<std::string>(channel_id);
                  auto vals = ReadIterator(it, true);
                  ASSERT_TRUE(OrderedVectorCompare({ "hello", "world", "." }, vals));
              };

    Execute(w0, w1);
}

TEST_F(DataManagerChannelFixture, Scatter_TwoWorkers_CompleteExchange) {
    Barrier sync(2);
    auto w0 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel(true);
                  sync.Await();
                  auto src_id = manager.AllocateDIA();
                  auto emitter = manager.GetLocalEmitter<std::string>(src_id);
                  emitter("foo");
                  emitter("bar");
                  emitter.Close();
                  manager.Scatter<std::string>(src_id, channel_id, { 0, 2 });
                  sync.Await();
                  auto it = manager.GetIterator<std::string>(channel_id);
                  auto vals = ReadIterator(it, true);
                  ASSERT_TRUE(OrderedVectorCompare({ "hello", "world", "." }, vals));
              };
    auto w1 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel(true);
                  sync.Await();
                  auto src_id = manager.AllocateDIA();
                  auto emitter = manager.GetLocalEmitter<std::string>(src_id);
                  emitter("hello");
                  emitter("world");
                  emitter(".");
                  emitter.Close();
                  manager.Scatter<std::string>(src_id, channel_id, { 3, 3 });
                  sync.Await();
                  auto it = manager.GetIterator<std::string>(channel_id);
                  auto vals = ReadIterator(it, true);
                  ASSERT_TRUE(OrderedVectorCompare({ "foo", "bar" }, vals));
              };

    Execute(w0, w1);
}

TEST_F(DataManagerChannelFixture, Scatter_ThreeWorkers_PartialExchange) {
    Barrier sync(3);
    auto w0 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel(true);
                  sync.Await();
                  auto src_id = manager.AllocateDIA();
                  auto emitter = manager.GetLocalEmitter<std::string>(src_id);
                  emitter("1");
                  emitter("2");
                  emitter.Close();
                  manager.Scatter<std::string>(src_id, channel_id, { 2, 2, 2 });
                  sync.Await();
                  auto it = manager.GetIterator<std::string>(channel_id);
                  auto vals = ReadIterator(it, true);
                  ASSERT_TRUE(OrderedVectorCompare({ "1", "2" }, vals));
              };
    auto w1 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel(true);
                  sync.Await();
                  auto src_id = manager.AllocateDIA();
                  auto emitter = manager.GetLocalEmitter<std::string>(src_id);
                  emitter("3");
                  emitter("4");
                  emitter("5");
                  emitter("6");
                  emitter.Close();
                  manager.Scatter<std::string>(src_id, channel_id, { 0, 2, 4 });
                  sync.Await();
                  auto it = manager.GetIterator<std::string>(channel_id);
                  auto vals = ReadIterator(it, true);
                  ASSERT_TRUE(OrderedVectorCompare({ "3", "4" }, vals));
              };
    auto w2 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel(true);
                  sync.Await();
                  auto src_id = manager.AllocateDIA();
                  auto emitter = manager.GetLocalEmitter<std::string>(src_id);
                  emitter.Close();
                  manager.Scatter<std::string>(src_id, channel_id, { 0, 0, 0 });
                  sync.Await();
                  auto it = manager.GetIterator<std::string>(channel_id);
                  auto vals = ReadIterator(it, true);
                  ASSERT_TRUE(OrderedVectorCompare({ "5", "6" }, vals));
              };

    Execute(w0, w1, w2);
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_IsFinishedOnlyIfAllEmittersAreClosed) {
    Barrier sync(2);
    auto w0 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  sync.Await();
                  auto emitters = manager.GetNetworkEmitters<int>(channel_id);
                  emitters[0].Close();
                  sync.Await();
                  std::this_thread::sleep_for(2ms);
                  ASSERT_TRUE(manager.GetIterator<int>(channel_id).IsFinished());
              };
    auto w1 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  sync.Await();
                  auto emitters = manager.GetNetworkEmitters<int>(channel_id);
                  emitters[0].Close();
                  emitters[1].Close();
                  sync.Await();
                  std::this_thread::sleep_for(2ms);
                  ASSERT_FALSE(manager.GetIterator<int>(channel_id).IsFinished());
              };
    Execute(w0, w1);
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_HasNextFalseWhenNotFlushed) {
    Barrier sync(2);
    auto w0 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto emitters = manager.GetNetworkEmitters<int>(channel_id);
                  emitters[1](42);
                  std::this_thread::sleep_for(2ms);
                  sync.Await();
              };
    auto w1 = [&sync](Manager& manager) {
                  sync.Await();
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto it = manager.GetIterator<int>(channel_id);
                  ASSERT_FALSE(it.HasNext());
              };
    Execute(w0, w1);
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_HasNextWhenFlushed) {
    Barrier sync(2);
    auto w0 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto emitters = manager.GetNetworkEmitters<int>(channel_id);
                  emitters[1](42);
                  emitters[1].Flush();
                  sync.Await();
              };
    auto w1 = [&sync](Manager& manager) {
                  std::this_thread::sleep_for(10ms);
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto it = manager.GetIterator<int>(channel_id);
                  sync.Await();
                  ASSERT_TRUE(it.HasNext());
              };
    Execute(w0, w1);
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_ReadsDataFromOneRemoteWorkerAndHasNoNextAfterwards) {
    Barrier sync(2);
    auto w0 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto emitters = manager.GetNetworkEmitters<int>(channel_id);
                  emitters[1](42);
                  emitters[1].Flush();
                  std::this_thread::sleep_for(5ms);
                  sync.Await();
              };
    auto w1 = [&sync](Manager& manager) {
                  sync.Await();
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto it = manager.GetIterator<int>(channel_id);
                  ASSERT_TRUE(it.HasNext());
                  ASSERT_EQ(42, it.Next());
                  ASSERT_FALSE(it.HasNext());
              };
    Execute(w0, w1);
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_ReadsDataFromOneRemoteWorkerMultipleFlushes) {
    Barrier sync(2);
    auto w0 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto emitters = manager.GetNetworkEmitters<int>(channel_id);
                  emitters[1](1);
                  emitters[1].Flush();
                  std::this_thread::sleep_for(5ms);
                  sync.Await();
                  emitters[1](2);
                  emitters[1](3);
                  emitters[1].Flush();
                  std::this_thread::sleep_for(5ms);
                  sync.Await();
                  emitters[1](4);
                  emitters[1](5);
                  emitters[1](6);
                  emitters[1].Flush();
                  std::this_thread::sleep_for(5ms);
                  sync.Await();
              };
    auto w1 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto it = manager.GetIterator<int>(channel_id);
                  sync.Await();
                  ASSERT_EQ(1, it.Next());
                  sync.Await();
                  ASSERT_TRUE(it.HasNext());
                  ASSERT_EQ(2, it.Next());
                  ASSERT_EQ(3, it.Next());
                  sync.Await();
                  ASSERT_TRUE(it.HasNext());
                  ASSERT_EQ(4, it.Next());
                  ASSERT_EQ(5, it.Next());
                  ASSERT_EQ(6, it.Next());
                  ASSERT_FALSE(it.HasNext());
              };
    Execute(w0, w1);
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_ReadsDataFromMultipleWorkers) {
    Barrier sync(3);
    auto w1 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto emitters = manager.GetNetworkEmitters<int>(channel_id);
                  emitters[0](2);
                  emitters[0](3);
                  emitters[0].Flush();
                  std::this_thread::sleep_for(2ms);
                  sync.Await();
              };
    auto w2 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto emitters = manager.GetNetworkEmitters<int>(channel_id);
                  emitters[0](1);
                  emitters[0](4);
                  emitters[0].Close();
                  std::this_thread::sleep_for(2ms);
                  sync.Await();
              };
    auto w0 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto it = manager.GetIterator<int>(channel_id);
                  sync.Await();
                  auto vals = ReadIterator(it);
                  ASSERT_TRUE(VectorCompare({ 1, 2, 3, 4 }, vals));
              };
    Execute(w0, w1, w2);
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_ReadsDataFromTwoChannels) {
    Barrier sync(3);
    auto w1 = [&sync](Manager& manager) {
                  auto channel_id1 = manager.AllocateNetworkChannel();
                  auto channel_id2 = manager.AllocateNetworkChannel();
                  auto emitters1 = manager.GetNetworkEmitters<int>(channel_id1);
                  auto emitters2 = manager.GetNetworkEmitters<int>(channel_id2);
                  emitters1[0](2);
                  emitters1[0](3);
                  emitters1[0].Close();
                  std::this_thread::sleep_for(2ms);
                  sync.Await();
                  emitters2[0](5);
                  emitters2[0](6);
                  emitters2[0].Flush();
                  std::this_thread::sleep_for(2ms);
                  sync.Await();
              };
    auto w2 = [&sync](Manager& manager) {
                  auto channel_id1 = manager.AllocateNetworkChannel();
                  auto channel_id2 = manager.AllocateNetworkChannel();
                  auto emitters1 = manager.GetNetworkEmitters<int>(channel_id1);
                  auto emitters2 = manager.GetNetworkEmitters<int>(channel_id2);
                  emitters1[0](1);
                  emitters1[0](4);
                  emitters1[0].Flush();
                  std::this_thread::sleep_for(2ms);
                  sync.Await();
                  emitters2[0](7);
                  emitters2[0](8);
                  emitters2[0].Close();
                  std::this_thread::sleep_for(2ms);
                  sync.Await();
              };
    auto w0 = [&sync](Manager& manager) {
                  auto channel_id1 = manager.AllocateNetworkChannel();
                  auto it1 = manager.GetIterator<int>(channel_id1);
                  sync.Await();
                  auto vals1 = ReadIterator(it1);
                  ASSERT_TRUE(VectorCompare({ 1, 2, 3, 4 }, vals1));

                  auto channel_id2 = manager.AllocateNetworkChannel();
                  auto it2 = manager.GetIterator<int>(channel_id2);
                  sync.Await();
                  auto vals2 = ReadIterator(it2);
                  ASSERT_TRUE(VectorCompare({ 5, 6, 7, 8 }, vals2));
              };
    Execute(w0, w1, w2);
}

TEST_F(DataManagerChannelFixture, GetNetworkBlocks_SendsDataToMultipleWorkers) {
    Barrier sync(3);
    auto w1 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto emitters = manager.GetNetworkEmitters<int>(channel_id);
                  emitters[0](10);
                  emitters[1](11);
                  emitters[2](12);
                  emitters[0].Flush();
                  emitters[1].Flush();
                  emitters[2].Close();
                  std::this_thread::sleep_for(2ms);
                  sync.Await();
                  auto it = manager.GetIterator<int>(channel_id);
                  auto vals = ReadIterator(it);
                  ASSERT_TRUE(VectorCompare({ 1, 11, 21 }, vals));
              };
    auto w2 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto emitters = manager.GetNetworkEmitters<int>(channel_id);
                  emitters[0](20);
                  emitters[1](21);
                  emitters[2](22);
                  emitters[0].Close();
                  emitters[1].Flush();
                  emitters[2].Flush();
                  std::this_thread::sleep_for(2ms);
                  sync.Await();
                  auto it = manager.GetIterator<int>(channel_id);
                  auto vals = ReadIterator(it);
                  ASSERT_TRUE(VectorCompare({ 2, 12, 22 }, vals));
              };
    auto w0 = [&sync](Manager& manager) {
                  auto channel_id = manager.AllocateNetworkChannel();
                  auto emitters = manager.GetNetworkEmitters<int>(channel_id);
                  emitters[0](0);
                  emitters[1](1);
                  emitters[2](2);
                  emitters[0].Flush();
                  emitters[1].Close();
                  emitters[2].Flush();
                  std::this_thread::sleep_for(2ms);
                  sync.Await();
                  auto it = manager.GetIterator<int>(channel_id);
                  auto vals = ReadIterator(it);
                  ASSERT_TRUE(VectorCompare({ 0, 10, 20 }, vals));
              };
    Execute(w0, w1, w2);
}
/******************************************************************************/
