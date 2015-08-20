/*******************************************************************************
 * tests/net/binary_builder_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/net/buffer_builder.hpp>
#include <thrill/net/buffer_reader.hpp>

using c7a::net::BufferBuilder;
using c7a::net::BufferRef;
using c7a::net::BufferReader;
using c7a::net::Buffer;

TEST(BufferBuilder, Test1) {
    // construct a binary blob
    BufferBuilder bb;
    {
        bb.Put<unsigned int>(1);
        bb.PutString("test");

        bb.PutVarint(42);
        bb.PutVarint(12345678);
    }

    // read binary block and verify content

    BufferRef bbr = BufferRef(bb);

    const unsigned char bb_data[] = {
        // bb.Put<unsigned int>(1)
        0x01, 0x00, 0x00, 0x00,
        // bb.PutString("test")
        0x04, 0x74, 0x65, 0x73, 0x74,
        // bb.PutVarint(42);
        0x2a,
        // bb.PutVarint(12345678);
        0xce, 0xc2, 0xf1, 0x05,
    };

    BufferRef bb_verify(bb_data, sizeof(bb_data));

    if (bbr != bb_verify)
        std::cout << bbr.ToString();

    ASSERT_EQ(bbr, bb_verify);

    // read binary block using binary_reader

    BufferReader br = BufferRef(bb);

    ASSERT_EQ(br.Get<unsigned int>(), 1u);
    ASSERT_EQ(br.GetString(), "test");
    ASSERT_EQ(br.GetVarint(), 42u);
    ASSERT_EQ(br.GetVarint(), 12345678u);

    ASSERT_TRUE(br.empty());

    // MOVE origin bb (which still exists) into a net::Buffer

    ASSERT_EQ(bb.size(), sizeof(bb_data));
    Buffer nb = bb.ToBuffer();

    ASSERT_EQ(bb.size(), 0u);
    ASSERT_EQ(nb.size(), sizeof(bb_data));
}

/******************************************************************************/
