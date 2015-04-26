#include "gtest/gtest.h"
#include "c7a/data/data_manager.hpp"

using namespace c7a::data;

struct DataManagerFixture : public ::testing::Test {
    DataManager manager;
    DIAId id = manager.AllocateDIA();
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

TEST_F(DataManagerFixture, EmittAndIterate_CorrectOrder) {
    auto id = manager.AllocateDIA();
    auto emitFn = manager.GetLocalEmitter<int>(id);
    emitFn(123);
    emitFn(22);
    auto it = manager.GetLocalBlocks<int>(id);
    ASSERT_EQ(123, it.Next());
    ASSERT_EQ(22, it.Next());
}

TEST_F(DataManagerFixture, AllocateMultiple) {
    auto id = manager.AllocateDIA();
    auto id2 = manager.AllocateDIA();
    auto id3 = manager.AllocateDIA();
    auto id4 = manager.AllocateDIA();
    auto id5 = manager.AllocateDIA();
}


TEST_F(DataManagerFixture, EmittAndIterate_ConcurrentAccess) {
    auto id = manager.AllocateDIA();
    auto emitFn = manager.GetLocalEmitter<int>(id);
    auto it = manager.GetLocalBlocks<int>(id);
    emitFn(123);
    ASSERT_EQ(123, it.Next());
    ASSERT_FALSE(it.HasNext());

    emitFn(22);
    ASSERT_TRUE(it.HasNext());
    ASSERT_EQ(22, it.Next());
}
