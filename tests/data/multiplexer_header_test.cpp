/*******************************************************************************
 * tests/data/multiplexer_header_test.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/data/multiplexer_header.hpp>

using namespace c7a;
using namespace c7a::data;

struct StreamTest : public::testing::Test {
    StreamTest() {
        candidate.channel_id = 2;
        candidate.size = 4;
        candidate.nitems = 5;
        candidate.sender_rank = 6;
    }

    struct ChannelBlockHeader candidate;
};

TEST_F(StreamTest, ChannelBlockHeaderParsesAndSerializesHeader) {
    net::BufferBuilder bb;
    candidate.Serialize(bb);
    net::Buffer b = bb.ToBuffer();

    data::ChannelBlockHeader result;
    net::BufferReader br(b);
    result.ParseHeader(br);

    ASSERT_EQ(candidate.channel_id, result.channel_id);
    ASSERT_EQ(candidate.size, result.size);
    ASSERT_EQ(candidate.nitems, result.nitems);
    ASSERT_EQ(candidate.sender_rank, result.sender_rank);
}

TEST_F(StreamTest, ChannelBlockHeaderIsEnd) {
    ASSERT_FALSE(candidate.IsEnd());
    candidate.size = 0;
    ASSERT_TRUE(candidate.IsEnd());
}

/******************************************************************************/
