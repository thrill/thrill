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

struct TestBinaryReader : public ::testing::Test {
    TestBinaryReader()
        : str1("foo"), str2(""), str3("thilda"), buffer(""), buffer2(""), reader(nullptr, 0), reader2(nullptr, 0) {
        bb.PutString(str1);
        bb2.PutString(str1).PutString(str2).PutString(str3);
        buffer = BinaryBuffer(bb);
        buffer2 = BinaryBuffer(bb2);
        reader = BinaryBufferReader(buffer);
        reader2 = BinaryBufferReader(buffer2);
    }
    BinaryBufferBuilder bb;
    BinaryBufferBuilder bb2;
    std::string str1;
    std::string str2;
    std::string str3;
    BinaryBuffer buffer;
    BinaryBuffer buffer2;
    BinaryBufferReader reader;
    BinaryBufferReader reader2;
};

TEST_F(TestBinaryReader, SeekStringElementsReturnsZeroForZero) {
    size_t out;
    ASSERT_EQ(reader.SeekStringElements(0, &out), 0u);
    ASSERT_EQ(out, 0u);
}


TEST_F(TestBinaryReader, SeekStringElementsToEnd) {
    size_t out;
    ASSERT_EQ(reader.SeekStringElements(100, &out), 1u);
}

TEST_F(TestBinaryReader, SeekStringElementsToEndReturnsCorrectBytes) {
    size_t out;
    reader2.SeekStringElements(100, &out);
    ASSERT_EQ(str1.size() + str2.size() + str3.size() + sizeof(uint8_t) * 3, out);
}

TEST_F(TestBinaryReader, SeekStringElementsToMiddle) {
    size_t out;
    ASSERT_EQ(reader2.SeekStringElements(2, &out), 2u);
}

TEST_F(TestBinaryReader, SeekStringElementsToMiddleReturnsCorrectBytes) {
    size_t out;
    reader2.SeekStringElements(2, &out);
    ASSERT_EQ(str1.size() + str2.size() + sizeof(uint8_t) * 2, out);
}

TEST_F(TestBinaryReader, SeekStringElementsFromMiddleReturnsCorrectBytes) {
    size_t out;

    (void)reader2.GetString();
    ASSERT_EQ(2, reader2.SeekStringElements(2, &out));
    ASSERT_EQ(str2.size() + str3.size() + sizeof(uint8_t) * 2, out);
}

/******************************************************************************/
