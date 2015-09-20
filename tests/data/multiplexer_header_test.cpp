/*******************************************************************************
 * tests/data/multiplexer_header_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/data/multiplexer_header.hpp>

using namespace thrill;
using namespace thrill::data;

struct StreamTest : public::testing::Test {
    StreamTest() {
        candidate.stream_id = 2;
        candidate.size = 4;
        candidate.num_items = 5;
        candidate.sender_rank = 6;
    }

    struct StreamBlockHeader candidate;
};

TEST_F(StreamTest, StreamBlockHeaderParsesAndSerializesHeader) {
    net::BufferBuilder bb;
    candidate.Serialize(bb);
    net::Buffer b = bb.ToBuffer();

    data::StreamBlockHeader result;
    net::BufferReader br(b);
    result.ParseHeader(br);

    ASSERT_EQ(candidate.stream_id, result.stream_id);
    ASSERT_EQ(candidate.size, result.size);
    ASSERT_EQ(candidate.num_items, result.num_items);
    ASSERT_EQ(candidate.sender_rank, result.sender_rank);
}

TEST_F(StreamTest, StreamBlockHeaderIsEnd) {
    ASSERT_FALSE(candidate.IsEnd());
    candidate.size = 0;
    ASSERT_TRUE(candidate.IsEnd());
}

/******************************************************************************/
