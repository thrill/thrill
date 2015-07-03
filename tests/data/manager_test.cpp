/*******************************************************************************
 * tests/data/manager_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "c7a/data/manager.hpp"
#include <c7a/net/dispatcher.hpp>
#include <c7a/net/channel_multiplexer.hpp>

using namespace c7a::data;
using namespace c7a::net;

struct DataManagerFixture : public::testing::Test {
    DataManagerFixture()
        : dispatcher(),
          manager(dispatcher),
          id(manager.AllocateDIA()) { }

    DispatcherThread dispatcher;
    Manager          manager;
    DIAId            id;
};

TEST_F(DataManagerFixture, GetIterator_FailsIfNotFound) {
    ASSERT_ANY_THROW(manager.GetIterator<int>(999));
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
    auto it = manager.GetIterator<int>(id);
    ASSERT_TRUE(it.HasNext());
    ASSERT_EQ(123, it.Next());
    ASSERT_EQ(22, it.Next());
}

TEST_F(DataManagerFixture, GetNumElements_EmptyDIA) {
    auto emitFn = manager.GetLocalEmitter<int>(id);
    emitFn.Close();
    ASSERT_EQ(0u, manager.GetNumElements(id));
}

TEST_F(DataManagerFixture, GetNumElements) {
    auto emitFn = manager.GetLocalEmitter<int>(id);
    emitFn(0);
    emitFn(1);
    emitFn.Flush();
    emitFn(2);
    emitFn.Close();
    ASSERT_EQ(3u, manager.GetNumElements(id));
}

TEST_F(DataManagerFixture, AllocateMultiple) {
    manager.AllocateDIA();
    manager.AllocateDIA();
    manager.AllocateDIA();
    manager.AllocateDIA();
    manager.AllocateDIA();
}

TEST_F(DataManagerFixture, EmittAndIterate_ConcurrentAccess) {
    auto it = manager.GetIterator<int>(id);
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
