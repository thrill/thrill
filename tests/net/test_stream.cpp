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
        candidate.num_elements = 3;
        candidate.boundaries = data;
    }

    size_t                   data[3] = { 22, 23, 55 };
    struct StreamBlockHeader candidate;
};

TEST_F(StreamTest, StreamBlockHeaderParsesAndSerializesIdAndNumElements) {
    auto seri = candidate.Serialize();
    struct StreamBlockHeader result;
    result.ParseIdAndNumElem(seri);

    ASSERT_EQ(candidate.channel_id, result.channel_id);
    ASSERT_EQ(candidate.num_elements, result.num_elements);
}

TEST_F(StreamTest, StreamBlockHeaderParsesAndSerializesBoundaries) {
    auto seri = candidate.Serialize();
    std::string part1 = seri.substr(0, sizeof(size_t) * 2);
    std::string part2 = seri.substr(part1.length(), seri.length() - part1.length());
    struct StreamBlockHeader result;
    result.ParseIdAndNumElem(part1);
    result.ParseBoundaries(part2);

    ASSERT_EQ(22, result.boundaries[0]);
    ASSERT_EQ(23, result.boundaries[1]);
    ASSERT_EQ(55, result.boundaries[2]);
}

TEST_F(StreamTest, StreamBlockHeaderParsesAndSerializesBoundariesIfEmpty) {
    candidate.num_elements = 0;
    auto seri = candidate.Serialize();
    struct StreamBlockHeader result;
    result.ParseIdAndNumElem(seri);
    result.ParseBoundaries(seri);

    ASSERT_EQ(0, result.num_elements);
}

TEST_F(StreamTest, StreamBlockHeaderIsStreamEnd) {
    ASSERT_FALSE(candidate.IsStreamEnd());
    candidate.num_elements = 0;
    ASSERT_TRUE(candidate.IsStreamEnd());
}

/******************************************************************************/
