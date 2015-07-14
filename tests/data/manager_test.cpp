/*******************************************************************************
 * tests/data/manager_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "c7a/data/manager.hpp"
#include <c7a/net/dispatcher.hpp>

using namespace c7a::data;
using namespace c7a::net;

struct DataManagerFixture : public::testing::Test {
    DataManagerFixture()
        : dispatcher("disptacher"),
          manager(dispatcher),
          id(manager.AllocateFileId()) { }

    DispatcherThread dispatcher;
    Manager          manager;
    DataId           id;
};

TEST_F(DataManagerFixture, GetFile_FailsIfNotFound) {
    ASSERT_ANY_THROW(manager.GetFile(999));
}

TEST_F(DataManagerFixture, AllocateTwice) {
    auto id1 = manager.AllocateFileId();
    auto id2 = manager.AllocateFileId();
    ASSERT_FALSE(id1 == id2);
}

/******************************************************************************/
