/*******************************************************************************
 * tests/data/buffer_chain_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "c7a/data/repository.hpp"

using namespace c7a::data;

TEST(DataId, PreIncrement) {
    DataId id(LOCAL, 0);
    id++;
    ASSERT_EQ(1u, id.identifier);
}

TEST(DataId, NotEqual_DifferentIdentifier) {
    DataId id1(LOCAL, 0);
    DataId id2(LOCAL, 1);
    ASSERT_FALSE(id1 == id2);
}

TEST(DataId, NotEqual_DifferentLocations) {
    DataId id1(LOCAL, 0);
    DataId id2(NETWORK, 0);
    ASSERT_FALSE(id1 == id2);
}

TEST(DataId, Equal) {
    DataId id1(LOCAL, 2);
    DataId id2(LOCAL, 2);
    ASSERT_TRUE(id1 == id2);
}

TEST(DataId, EqualAfterPreIncrement) {
    DataId id1(LOCAL, 1);
    DataId id2(LOCAL, 2);
    ASSERT_TRUE(++id1 == id2);
}

/******************************************************************************/
