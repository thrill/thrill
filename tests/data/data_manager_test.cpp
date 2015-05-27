/*******************************************************************************
 * tests/data/data_manager_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "c7a/data/data_manager.hpp"
#include <c7a/net/net_dispatcher.hpp>
#include <c7a/net/channel_multiplexer.hpp>

using namespace c7a::data;
using namespace c7a::net;

struct DataManagerFixture : public::testing::Test {
    DataManagerFixture()
        : dispatcher(),
          cmp(dispatcher),
          manager(cmp),
          id(manager.AllocateDIA()) { }

    NetDispatcher      dispatcher;
    ChannelMultiplexer cmp;
    DataManager        manager;
    DIAId              id;
};

TEST_F(DataManagerFixture, GetLocalBlock_FailsIfNotFound) {
    ASSERT_ANY_THROW(manager.GetLocalBlocks<int>(999));
}

TEST_F(DataManagerFixture, GetLocalEmitter_FailsIfNotFound) {
    ASSERT_ANY_THROW(manager.GetLocalEmitter<int>(23));
}

TEST_F(DataManagerFixture, GetLocalEmitter_CanCallEmitter) {
    auto e = manager.GetLocalEmitter<int>(manager.AllocateDIA());
    ASSERT_NO_THROW(e(123));
}
TEST_F(DataManagerFixture, AllocateTwice) {
    manager.AllocateDIA();
    manager.AllocateDIA();
}
TEST_F(DataManagerFixture, EmittAndIterate_CorrectOrder) {
    auto emitFn = manager.GetLocalEmitter<int>(id);
    emitFn(123);
    emitFn(22);
    emitFn.Flush();
    auto it = manager.GetLocalBlocks<int>(id);
    ASSERT_TRUE(it.HasNext());
    ASSERT_EQ(123, it.Next());
    ASSERT_EQ(22, it.Next());
}

TEST_F(DataManagerFixture, AllocateMultiple) {
    manager.AllocateDIA();
    manager.AllocateDIA();
    manager.AllocateDIA();
    manager.AllocateDIA();
    manager.AllocateDIA();
}

TEST_F(DataManagerFixture, EmittAndIterate_ConcurrentAccess) {
    auto it = manager.GetLocalBlocks<int>(id);
    auto emitFn = manager.GetLocalEmitter<int>(id);
    emitFn(123);
    emitFn.Flush();
    ASSERT_TRUE(it.HasNext());
    ASSERT_EQ(123, it.Next());
    ASSERT_FALSE(it.HasNext());

    emitFn(22);
    emitFn.Flush();
    ASSERT_TRUE(it.HasNext());
    ASSERT_EQ(22, it.Next());
}

/******************************************************************************/
