/*******************************************************************************
 * tests/data/buffer_chain_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
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

TEST(BufferChain, Size_0_when_empty) {
    BufferChain candidate;
    ASSERT_EQ(0u, candidate.size());
}

TEST(BufferChain, Size_0_WhenAppendedEmptyBuffer) {
    BufferChain candidate;
    BinaryBufferBuilder b(0);
    candidate.Append(b);
    ASSERT_EQ(0u, candidate.size());
}

TEST(BufferChain, Size_2_WhenAppendedTwoBuffersOneElementEach) {
    BufferChain candidate;
    BinaryBufferBuilder b1, b2;
    b1.set_elements(1);
    b2.set_elements(1);
    candidate.Append(b1);
    candidate.Append(b2);
    ASSERT_EQ(2u, candidate.size());
}

/******************************************************************************/
