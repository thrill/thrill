/*******************************************************************************
 * tests/net/test_stream.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include <c7a/net/stream.hpp>

using::testing::_;
using::testing::InvokeArgument;
using::testing::Return;
using::testing::ByRef;

using namespace c7a;
using namespace c7a::net;

struct StreamTest : public::testing::Test {
    StreamTest()
    {
        candidate.channel_id = 2;
        candidate.expected_bytes = 4;
    }

    struct StreamBlockHeader candidate;
};

TEST_F(StreamTest, StreamBlockHeaderParsesAndSerializesHeader) {
    auto seri = candidate.Serialize();
    struct StreamBlockHeader result;
    result.ParseHeader(seri);

    ASSERT_EQ(candidate.channel_id, result.channel_id);
    ASSERT_EQ(candidate.expected_bytes, result.expected_bytes);
}

TEST_F(StreamTest, StreamBlockHeaderIsStreamEnd) {
    ASSERT_FALSE(candidate.IsStreamEnd());
    candidate.expected_bytes = 0;
    ASSERT_TRUE(candidate.IsStreamEnd());
}

/******************************************************************************/
