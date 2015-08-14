/*******************************************************************************
 * tests/data/file_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/string.hpp>
#include <c7a/data/block_queue.hpp>
#include <c7a/data/file.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using namespace c7a;

struct File : public::testing::Test {
    data::BlockPool block_pool_ { nullptr };
};

TEST_F(File, PutSomeItemsGetItems) {

    // construct File with very small blocks for testing
    data::File file(block_pool_);

    {
        data::File::Writer fw = file.GetWriter(16);
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

    ASSERT_EQ(file.block(0).size(), 16u);
    ASSERT_EQ(file.block(1).size(), 16u);
    ASSERT_EQ(file.block(2).size(), 16u);
    ASSERT_EQ(file.block(3).size(), 16u);
    ASSERT_EQ(file.block(4).size(), 16u);
    ASSERT_EQ(file.block(5).size(), 14u);

    //Total size is equal to sum of block sizes
    ASSERT_EQ(file.TotalSize(), 94u);

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
            std::cout << common::hexdump(file.block(i).ToString())
                      << std::endl;
        }
    }

    std::string block_data(reinterpret_cast<const char*>(block_data_bytes),
                           sizeof(block_data_bytes));

    // compare frozen byte data with File contents

    ASSERT_EQ(block_data, file.ReadComplete());

    // check size of Block.
    {
        data::ByteBlockCPtr bytes = file.block(0).byte_block();
        ASSERT_EQ(16u, bytes->size());
    }

    // read File contents using BlockReader
    {
        data::File::Reader fr = file.GetReader();
        ASSERT_EQ(fr.Read(8), "testtest");
        ASSERT_EQ(fr.GetVarint(), 123456u);
        ASSERT_EQ(fr.GetString(), "test1test2test3");
        ASSERT_EQ(fr.GetString(), std::string(64, '1'));
        ASSERT_EQ(fr.Get<uint16_t>(), 42);
        ASSERT_THROW(fr.Get<uint16_t>(), std::runtime_error);
    }
}

TEST_F(File, SerializeSomeItems) {

    // construct File with very small blocks for testing
    data::File file(block_pool_);

    using MyPair = std::pair<int, std::string>;

    // put into File some items (all of different serialization bytes)
    {
        // construct File with very small blocks for testing
        data::File::Writer fw = file.GetWriter(1024);
        fw(static_cast<unsigned>(5));
        fw(MyPair(5, "10abc"));
        fw(static_cast<double>(42.0));
        fw(std::string("test"));
    }

    //std::cout << common::hexdump(file.BlockAsString(0)) << std::endl;

    // get items back from file.
    {
        data::File::Reader fr = file.GetReader();
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

TEST_F(File, SerializeSomeItemsDynReader) {

    // construct File with very small blocks for testing
    data::File file(block_pool_);

    using MyPair = std::pair<int, std::string>;

    // put into File some items (all of different serialization bytes)
    {
        // construct File with very small blocks for testing
        data::File::Writer fw = file.GetWriter(1024);
        fw(static_cast<unsigned>(5));
        fw(MyPair(5, "10abc"));
        fw(static_cast<double>(42.0));
        fw(std::string("test"));
    }

    // get items back from file.
    {
        data::File::DynReader fr = file.GetDynReader();
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

TEST_F(File, SeekReadSlicesOfFiles) {
    static const bool debug = false;

    // construct a small-block File with lots of items.
    data::File file(block_pool_);

    // yes, this is a prime number as block size. -tb
    data::File::Writer fw = file.GetWriter(53);
    for (size_t i = 0; i < 1000; ++i) {
        fw(i);
    }
    fw.Close();

    ASSERT_EQ(1000u, file.NumItems());

    // read complete File
    data::File::Reader fr = file.GetReader();
    for (size_t i = 0; i < 1000; ++i) {
        ASSERT_TRUE(fr.HasNext());
        ASSERT_EQ(i, fr.Next<size_t>());
    }
    ASSERT_FALSE(fr.HasNext());

    // read items 95-144
    auto check_range =
        [&](size_t begin, size_t end, bool do_more = true) {
            LOG << "Test range [" << begin << "," << end << ")";

            // seek in File to begin.
            data::File::Reader fr = file.GetReaderAt<size_t>(begin);

            // read a few items
            if (end - begin > 5 && do_more) {
                for (size_t i = 0; i < 5; ++i) {
                    ASSERT_TRUE(fr.HasNext());
                    ASSERT_EQ(begin, fr.Next<size_t>());
                    ++begin;
                }
            }

            // read the items [begin,end)
            {
                std::vector<data::Block> blocks
                    = fr.GetItemBatch<size_t>(end - begin);

                data::BlockQueue queue(block_pool_);

                for (data::Block& b : blocks)
                    queue.AppendBlock(b);
                queue.Close();

                data::BlockQueue::Reader qr = queue.GetReader();

                for (size_t i = begin; i < end; ++i) {
                    ASSERT_TRUE(qr.HasNext());
                    ASSERT_EQ(i, qr.Next<size_t>());
                }
                ASSERT_FALSE(qr.HasNext());
            }

            if (!do_more) return;

            sLOG << "read more";
            static const size_t more = 100;

            // read the items [end, end + more)
            {
                std::vector<data::Block> blocks
                    = fr.GetItemBatch<size_t>(more);

                data::BlockQueue queue(block_pool_);

                for (data::Block& b : blocks)
                    queue.AppendBlock(b);
                queue.Close();

                data::BlockQueue::Reader qr = queue.GetReader();

                for (size_t i = end; i < end + more; ++i) {
                    ASSERT_TRUE(qr.HasNext());
                    ASSERT_EQ(i, qr.Next<size_t>());
                }
                ASSERT_FALSE(qr.HasNext());
            }
        };

    // read some item ranges.
    for (size_t i = 90; i != 100; ++i) {
        check_range(i, 144);
    }
    for (size_t i = 140; i != 150; ++i) {
        check_range(96, i);
    }

    // some special cases.
    check_range(0, 0);
    check_range(0, 1);
    check_range(1, 2);
    check_range(990, 1000, false);
    check_range(1000, 1000, false);
}

// forced instantiation
template class data::BlockReader<data::FileBlockSource>;

// fixed size serialization test
static_assert(data::Serialization<data::BlockWriter, int>
              ::is_fixed_size == true, "");
static_assert(data::Serialization<data::BlockWriter, int>
              ::fixed_size == sizeof(int), "");

static_assert(data::Serialization<data::BlockWriter, std::string>
              ::is_fixed_size == false, "");

static_assert(data::Serialization<data::BlockWriter, std::pair<int, short> >
              ::is_fixed_size == true, "");
static_assert(data::Serialization<data::BlockWriter, std::pair<int, short> >
              ::fixed_size == sizeof(int) + sizeof(short), "");

static_assert(data::Serialization<data::BlockWriter, std::pair<int, std::string> >
              ::is_fixed_size == false, "");

/******************************************************************************/
