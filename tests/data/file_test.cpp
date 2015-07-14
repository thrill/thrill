/*******************************************************************************
 * tests/data/file_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/data/file.hpp>
#include <c7a/common/string.hpp>
#include <gtest/gtest.h>

using namespace c7a;

TEST(File, PutSomeItemsGetItems) {

    // construct File with very small blocks for testing
    using File = data::FileBase<16>;
    File file;

    {
        File::Writer fw(file);
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

    // check size of Block.
    {
        File::BlockCPtr block = file.block(0);
        static_assert(sizeof(*block) == 16, "Block size does not match");
    }

    // read File contents using BlockReader
    {
        File::Reader fr = file.GetReader();
        ASSERT_EQ(fr.Read(8), "testtest");
        ASSERT_EQ(fr.GetVarint(), 123456u);
        ASSERT_EQ(fr.GetString(), "test1test2test3");
        ASSERT_EQ(fr.GetString(), std::string(64, '1'));
        ASSERT_EQ(fr.Get<uint16_t>(), 42);
        ASSERT_THROW(fr.Get<uint16_t>(), std::runtime_error);
    }
}

TEST(File, SerializeSomeItems) {

    // construct File with very small blocks for testing
    using File = data::FileBase<1024>;
    File file;

    using MyPair = std::pair<int, std::string>;

    // put into File some items (all of different serialization bytes)
    {
        File::Writer fw(file);
        fw(unsigned(5));
        fw(MyPair(5, "10abc"));
        fw(double(42.0));
        fw(std::string("test"));
    }

    //std::cout << common::hexdump(file.BlockAsString(0)) << std::endl;

    // get items back from file.
    {
        File::Reader fr = file.GetReader();
        unsigned i1 = fr.Next<unsigned>();
        ASSERT_EQ(i1, 5u);
        MyPair i2 = fr.Next<MyPair>();
        ASSERT_EQ(i2, MyPair(5, "10abc"));
        double i3 = fr.Next<double>();
        ASSERT_DOUBLE_EQ(i3, 42.0);
        std::string i4 = fr.Next<std::string>();
        ASSERT_EQ(i4, "test");
    }
}

// forced instantiation
using MyBlock = data::Block<16>;
template class data::FileBase<16>;
template class data::BlockWriter<MyBlock, data::FileBase<16> >;
template class data::BlockReader<16>;

// fixed size test
using MyWriter = data::BlockWriter<MyBlock, data::FileBase<16> >;
using MyReader = data::BlockReader<16>;
static_assert(data::Serializer<MyWriter, int>
              ::fixed_size == true, "");
static_assert(data::Serializer<MyWriter, std::string>
              ::fixed_size == false, "");
static_assert(data::Serializer<MyWriter, std::pair<int, short> >
              ::fixed_size == true, "");
static_assert(data::Serializer<MyWriter, std::pair<int, std::string> >
              ::fixed_size == false, "");

/******************************************************************************/
