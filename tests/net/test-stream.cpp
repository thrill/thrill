/*******************************************************************************
 * tests/net/test-stream.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include <c7a/net/stream.hpp>

using::testing::_;
using::testing::InvokeArgument;
using::testing::Return;
using::testing::ByRef;

using namespace c7a::net;
using namespace c7a;


struct StreamTest : public::testing::Test {
    StreamTest() {
        candidate.channel_id = 2;
        candidate.num_elements = 3;
       // candidate.boundaries = data;
    }

    //size_t data[3] = {22, 23, 55};
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
    struct StreamBlockHeader result;
    result.ParseIdAndNumElem(seri);

    ASSERT_EQ(22, result.boundaries[0]);
    ASSERT_EQ(23, result.boundaries[0]);
    ASSERT_EQ(55, result.boundaries[0]);
}

