/*******************************************************************************
 * tests/data/file_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/data/file.hpp>
#include <c7a/common/string.hpp>
#include <gtest/gtest.h>

using namespace c7a;

TEST(File, PutSomeItemsGetItems) {

    // construct File with very small blocks for testing
    using File = data::File<16>;
    File file;

    {
        File::Writer fw = file.GetWriter();
        fw.MarkItem();
        fw.Append("testtest");
        fw.MarkItem();
        fw.PutVarint(123456u);
        fw.MarkItem();
        fw.PutString("test1test2test3");
        fw.MarkItem();
        // long item spanning multiple blocks
        fw.PutString(std::string(64, '1'));
        fw.MarkItem();
        fw.Put<uint16_t>(42);
    }

    ASSERT_EQ(file.NumBlocks(), 6u);
    ASSERT_EQ(file.NumItems(), 5u);
    ASSERT_EQ(file.TotalBytes(), 6u * 16u);

    ASSERT_EQ(file.used(0), 16u);
    ASSERT_EQ(file.used(1), 16u);
    ASSERT_EQ(file.used(2), 16u);
    ASSERT_EQ(file.used(3), 16u);
    ASSERT_EQ(file.used(4), 16u);
    ASSERT_EQ(file.used(5), 14u);

    const unsigned char block_data_bytes[] = {
        // fw.Append("testtest");
        0x74, 0x65, 0x73, 0x74, 0x74, 0x65, 0x73, 0x74,
        // fw.PutVarint(123456u);
        0xC0, 0xC4, 0x07,
        // fw.PutString("test1test2test3");
        0x0F,
        0x74, 0x65, 0x73, 0x74, 0x31, 0x74, 0x65, 0x73,
        0x74, 0x32, 0x74, 0x65, 0x73, 0x74, 0x33,
        // fw.PutString(std::string(64, '1'));
        0x40,
        0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31,
        0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31,
        0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31,
        0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31,
        0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31,
        0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31,
        0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31,
        0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31, 0x31,
        // fw.Put<uint16_t>(42);
        0x2A, 0x00
    };

    if (0) {
        for (size_t i = 0; i != file.NumBlocks(); ++i) {
            std::cout << common::hexdump(file.BlockAsString(i))
                      << std::endl;
        }
    }

    std::string block_data(reinterpret_cast<const char*>(block_data_bytes),
                           sizeof(block_data_bytes));

    // compare frozen byte data with File contents

    for (size_t i = 0; i != file.NumBlocks(); ++i) {
        ASSERT_EQ(
            block_data.substr(
                i * File::block_size,
                std::min<size_t>(File::block_size, file.used(i))
                ),
            file.BlockAsString(i));
    }

    // read File contents using BlockReader
    {
        File::Reader fr = file.ReaderAtStart();
        ASSERT_EQ(fr.Read(8), "testtest");
        ASSERT_EQ(fr.GetVarint(), 123456u);
        ASSERT_EQ(fr.GetString(), "test1test2test3");
        ASSERT_EQ(fr.GetString(), std::string(64, '1'));
        ASSERT_EQ(fr.Get<uint16_t>(), 42);
        ASSERT_THROW(fr.Get<uint16_t>(), std::runtime_error);
    }
}

// forced instantiation
using MyBlock = data::Block<16>;
template class data::File<16>;
template class data::BlockWriter<MyBlock, data::File<16> >;
template class data::BlockReader<16>;

/******************************************************************************/
