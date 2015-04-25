#include "gtest/gtest.h"
#include "c7a/data/data_manager.hpp"

using namespace c7a::data;

TEST(DataManager, GetLocalBlock_FailsIfNotFound) {
    DataManager manager;
    ASSERT_ANY_THROW(manager.getLocalBlocks<int>(0));
}

TEST(DataManager, GetLocalEmitter_FailsIfNotFound) {
    DataManager manager;
    ASSERT_ANY_THROW(manager.getLocalEmitter<int>(0));
}
