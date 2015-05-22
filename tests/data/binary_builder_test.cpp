/*******************************************************************************
 * tests/data/binary_builder_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/data/binary_buffer_builder.hpp>
#include <c7a/data/binary_buffer.hpp>
#include <c7a/data/binary_buffer_reader.hpp>
#include <gtest/gtest.h>

using c7a::data::BinaryBufferBuilder;
using c7a::data::BinaryBuffer;
using c7a::data::BinaryBufferReader;
using c7a::net::Buffer;

TEST(BinaryBufferBuilder, Test1) {
    // construct a binary blob
    BinaryBufferBuilder bb;
    {
        bb.Put<unsigned int>(1);
        bb.PutString("test");

        bb.PutVarint(42);
        bb.PutVarint(12345678);

        // add a sub block
        BinaryBufferBuilder sub;
        sub.PutString("sub block");
        sub.PutVarint(6 * 9);

        bb.PutString(sub);
    }

    // read binary block and verify content

    BinaryBuffer bbr = BinaryBuffer(bb);

    const unsigned char bb_data[] = {
        // bb.Put<unsigned int>(1)
        0x01, 0x00, 0x00, 0x00,
        // bb.PutString("test")
        0x04, 0x74, 0x65, 0x73, 0x74,
        // bb.PutVarint(42);
        0x2a,
        // bb.PutVarint(12345678);
        0xce, 0xc2, 0xf1, 0x05,
        // begin sub block (length)
        0x0b,
        // sub.PutString("sub block");
        0x09, 0x73, 0x75, 0x62, 0x20, 0x62, 0x6c, 0x6f, 0x63, 0x6b,
        // sub.PutVarint(6 * 9);
        0x36,
    };

    BinaryBuffer bb_verify(bb_data, sizeof(bb_data));

    if (bbr != bb_verify)
        std::cout << bbr.ToString();

    ASSERT_EQ(bbr, bb_verify);

    // read binary block using binary_reader

    BinaryBufferReader br = BinaryBuffer(bb);

    ASSERT_EQ(br.Get<unsigned int>(), 1u);
    ASSERT_EQ(br.GetString(), "test");
    ASSERT_EQ(br.GetVarint(), 42u);
    ASSERT_EQ(br.GetVarint(), 12345678u);

    {
        BinaryBufferReader sub_br = br.GetBinaryBuffer();
        ASSERT_EQ(sub_br.GetString(), "sub block");
        ASSERT_EQ(sub_br.GetVarint(), 6 * 9u);
        ASSERT_TRUE(sub_br.empty());
    }

    ASSERT_TRUE(br.empty());

    // MOVE origin bb (which still exists) into a net::Buffer

    ASSERT_EQ(bb.size(), sizeof(bb_data));
    Buffer nb = bb.ToBuffer();

    ASSERT_EQ(bb.size(), 0u);
    ASSERT_EQ(nb.size(), sizeof(bb_data));
}

/******************************************************************************/
