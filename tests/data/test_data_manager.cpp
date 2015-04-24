#include "gtest/gtest.h"
#include "c7a/data/data_manager.hpp"

using namespace c7a::data;

TEST(DataManager, GetLocalBlockReturnsEmptyIteratorIfNotFound) {
    DataManager manager;
    auto it = manager.getLocalBlocks<int>(0);
    ASSERT_FALSE(it.has_next());
}
