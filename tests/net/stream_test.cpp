/*******************************************************************************
 * tests/net/stream_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include <c7a/data/stream_block_header.hpp>

using::testing::_;
using::testing::InvokeArgument;
using::testing::Return;
using::testing::ByRef;

using namespace c7a;
using namespace c7a::net;

struct StreamTest : public::testing::Test {
    StreamTest() {
        candidate.channel_id = 2;
        candidate.expected_bytes = 4;
        candidate.expected_elements = 5;
        candidate.sender_rank = 6;
    }

    struct StreamBlockHeader candidate;
};

TEST_F(StreamTest, StreamBlockHeaderParsesAndSerializesHeader) {
    auto seri = candidate.Serialize();
    struct StreamBlockHeader result;
    result.ParseHeader(seri);

    ASSERT_EQ(candidate.channel_id, result.channel_id);
    ASSERT_EQ(candidate.expected_bytes, result.expected_bytes);
    ASSERT_EQ(candidate.expected_elements, result.expected_elements);
    ASSERT_EQ(candidate.sender_rank, result.sender_rank);
}

TEST_F(StreamTest, StreamBlockHeaderIsStreamEnd) {
    ASSERT_FALSE(candidate.IsStreamEnd());
    candidate.expected_bytes = 0;
    ASSERT_TRUE(candidate.IsStreamEnd());
}

/******************************************************************************/
