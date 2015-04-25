#include "gtest/gtest.h"
#include "c7a/data/data_manager.hpp"

using namespace c7a::data;

TEST(DataManager, GetLocalBlock_FailsIfNotFound) {
    DataManager manager;
    ASSERT_ANY_THROW(manager.GetLocalBlocks<int>(0));
}

TEST(DataManager, GetLocalEmitter_FailsIfNotFound) {
    DataManager manager;
    ASSERT_ANY_THROW(manager.GetLocalEmitter<int>(0));
}

TEST(DataManager, GetLocalEmitter_CanCallEmitter) {
    DataManager manager;
    auto e = manager.GetLocalEmitter<int>(manager.AllocateDIA());
    ASSERT_NO_THROW(e(123));
}

TEST(DataManager, EmittAndIterate_CorrectOrder) {
    DataManager manager;
    auto id = manager.AllocateDIA();
    auto emitFn = manager.GetLocalEmitter<int>(id);
    emitFn(123);
    emitFn(22);
    auto it = manager.GetLocalBlocks<int>(id);
    ASSERT_EQ(123, it.Next());
    ASSERT_EQ(22, it.Next());
}

TEST(DataManager, EmittAndIterate_ConcurrentAccess) {
    DataManager manager;
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
