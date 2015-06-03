/*******************************************************************************
 * tests/data/buffer_chain_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "c7a/data/buffer_chain_manager.hpp"

using namespace c7a::data;

TEST(ChainId, PreIncrement) {
    ChainId id(LOCAL, 0);
    id++;
    ASSERT_EQ(1u, id.identifier);
}

TEST(ChainId, NotEqual_DifferentIdentifier) {
    ChainId id1(LOCAL, 0);
    ChainId id2(LOCAL, 1);
    ASSERT_FALSE(id1 == id2);
}

TEST(ChainId, NotEqual_DifferentLocations) {
    ChainId id1(LOCAL, 0);
    ChainId id2(NETWORK, 0);
    ASSERT_FALSE(id1 == id2);
}

TEST(ChainId, Equal) {
    ChainId id1(LOCAL, 2);
    ChainId id2(LOCAL, 2);
    ASSERT_TRUE(id1 == id2);
}

TEST(ChainId, EqualAfterPreIncrement) {
    ChainId id1(LOCAL, 1);
    ChainId id2(LOCAL, 2);
    ASSERT_TRUE(++id1 == id2);
}

/******************************************************************************/
