/*******************************************************************************
 * tests/data/stream_block_header_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/data/stream_block_header.hpp>
#include <gtest/gtest.h>

using namespace c7a;
using namespace c7a::data;

struct StreamTest : public::testing::Test {
    StreamTest() {
        candidate.channel_id = 2;
        candidate.size = 4;
        candidate.nitems = 5;
        candidate.sender_rank = 6;
    }

    struct StreamBlockHeader candidate;
};

TEST_F(StreamTest, StreamBlockHeaderParsesAndSerializesHeader) {
    auto seri = candidate.Serialize();
    struct StreamBlockHeader result;
    result.ParseHeader(seri);

    ASSERT_EQ(candidate.channel_id, result.channel_id);
    ASSERT_EQ(candidate.size, result.size);
    ASSERT_EQ(candidate.nitems, result.nitems);
    ASSERT_EQ(candidate.sender_rank, result.sender_rank);
}

TEST_F(StreamTest, StreamBlockHeaderIsStreamEnd) {
    ASSERT_FALSE(candidate.IsStreamEnd());
    candidate.size = 0;
    ASSERT_TRUE(candidate.IsStreamEnd());
}

/******************************************************************************/
