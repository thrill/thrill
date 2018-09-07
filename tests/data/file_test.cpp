/*******************************************************************************
 * tests/data/file_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/string.hpp>
#include <thrill/data/block_queue.hpp>
#include <thrill/data/file.hpp>

#include <tlx/string/hexdump.hpp>

#include <algorithm>
#include <functional>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;

struct File : public ::testing::Test {
    data::BlockPool block_pool_;
};

TEST_F(File, PutSomeItemsGetItems) {

    // construct File with very small blocks for testing
    data::File file(block_pool_, 0, /* dia_id */ 0);

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
        fw.PutRaw<uint16_t>(42);
    }

    ASSERT_EQ(file.num_blocks(), 6u);
    ASSERT_EQ(file.num_items(), 5u);

    ASSERT_EQ(file.block(0).size(), 16u);
    ASSERT_EQ(file.block(1).size(), 16u);
    ASSERT_EQ(file.block(2).size(), 16u);
    ASSERT_EQ(file.block(3).size(), 16u);
    ASSERT_EQ(file.block(4).size(), 16u);
    ASSERT_EQ(file.block(5).size(), 14u);

    // Total size is equal to sum of block sizes
    ASSERT_EQ(file.size_bytes(), 94u);

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
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        // fw.Put<uint16_t>(42);
        0x2A, 0x00
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        // fw.Put<uint16_t>(42);
        0x00, 0x2A
#endif
    };

    if (0) {
        for (size_t i = 0; i != file.num_blocks(); ++i) {
            std::cout << tlx::hexdump(file.block(i).PinWait(0).ToString())
                      << std::endl;
        }
    }

    std::string block_data(reinterpret_cast<const char*>(block_data_bytes),
                           sizeof(block_data_bytes));

    // compare frozen byte data with File contents

    ASSERT_EQ(block_data, file.ReadComplete());

    // check size of Block.
    {
        data::ByteBlockPtr bytes = file.block(0).byte_block();
        ASSERT_EQ(16u, bytes->size());
    }

    // read File contents using BlockReader
    {
        data::File::KeepReader fr = file.GetKeepReader();
        ASSERT_EQ(fr.Read(8), "testtest");
        ASSERT_EQ(fr.GetVarint(), 123456u);
        ASSERT_EQ(fr.GetString(), "test1test2test3");
        ASSERT_EQ(fr.GetString(), std::string(64, '1'));
        ASSERT_EQ(fr.GetRaw<uint16_t>(), 42);
        ASSERT_THROW(fr.GetRaw<uint16_t>(), std::runtime_error);
    }
}

TEST_F(File, WriteZeroItems) {

    // construct File with very small blocks for testing
    data::File file(block_pool_, 0, /* dia_id */ 0);

    {
        // construct File with very small blocks for testing
        data::File::Writer fw = file.GetWriter(1024);

        // but dont write anything
        fw.Close();
    }

    // get zero items back from file.
    {
        data::File::KeepReader fr = file.GetKeepReader();

        ASSERT_FALSE(fr.HasNext());
    }
}

TEST_F(File, SerializeSomeItems) {

    // construct File with very small blocks for testing
    data::File file(block_pool_, 0, /* dia_id */ 0);

    using MyPair = std::pair<int, std::string>;

    // put into File some items (all of different serialization bytes)
    {
        // construct File with very small blocks for testing
        data::File::Writer fw = file.GetWriter(1024);
        fw.Put(static_cast<unsigned>(5));
        fw.Put(MyPair(5, "10abc"));
        fw.Put(static_cast<double>(42.0));
        fw.Put(std::string("test"));
    }

    // std::cout << common::hexdump(file.BlockAsString(0)) << std::endl;

    // get items back from file.
    {
        data::File::KeepReader fr = file.GetKeepReader();
        ASSERT_TRUE(fr.HasNext());
        unsigned i1 = fr.Next<unsigned>();
        ASSERT_EQ(i1, 5u);

        ASSERT_TRUE(fr.HasNext());
        MyPair i2 = fr.Next<MyPair>();
        ASSERT_EQ(i2, MyPair(5, "10abc"));

        ASSERT_TRUE(fr.HasNext());
        double i3 = fr.Next<double>();
        ASSERT_DOUBLE_EQ(i3, 42.0);

        ASSERT_TRUE(fr.HasNext());
        std::string i4 = fr.Next<std::string>();
        ASSERT_EQ(i4, "test");

        ASSERT_TRUE(!fr.HasNext());
    }
}

TEST_F(File, SerializeSomeItemsDynReader) {

    // construct File with very small blocks for testing
    data::File file(block_pool_, 0, /* dia_id */ 0);

    using MyPair = std::pair<int, std::string>;

    // put into File some items (all of different serialization bytes)
    {
        // construct File with very small blocks for testing
        data::File::Writer fw = file.GetWriter(1024);
        fw.Put(static_cast<unsigned>(5));
        fw.Put(MyPair(5, "10abc"));
        fw.Put(static_cast<double>(42.0));
        fw.Put(std::string("test"));
    }
    ASSERT_EQ(4u, file.num_items());

    // get items back from file.
    {
        data::File::Reader fr = file.GetReader(false);
        ASSERT_TRUE(fr.HasNext());
        unsigned i1 = fr.Next<unsigned>();
        ASSERT_EQ(i1, 5u);

        ASSERT_TRUE(fr.HasNext());
        MyPair i2 = fr.Next<MyPair>();
        ASSERT_EQ(i2, MyPair(5, "10abc"));

        ASSERT_TRUE(fr.HasNext());
        double i3 = fr.Next<double>();
        ASSERT_DOUBLE_EQ(i3, 42.0);

        ASSERT_TRUE(fr.HasNext());
        std::string i4 = fr.Next<std::string>();
        ASSERT_EQ(i4, "test");

        ASSERT_TRUE(!fr.HasNext());
    }
    ASSERT_EQ(4u, file.num_items());
}

TEST_F(File, SerializeSomeItemsConsumeReader) {
    static constexpr size_t size = 5000;

    // construct File with very small blocks for testing
    data::File file(block_pool_, 0, /* dia_id */ 0);

    // put into File some items (all of different serialization bytes)
    {
        // construct File with very small blocks for testing
        data::File::Writer fw = file.GetWriter(53);
        for (unsigned i = 0; i < size; ++i) {
            fw.Put<unsigned>(i);
        }
    }

    // get items back from file, consuming it.
    {
        data::File::Reader fr = file.GetReader(true);
        for (size_t i = 0; i < size; ++i) {
            ASSERT_TRUE(fr.HasNext());
            unsigned iread = fr.Next<unsigned>();
            ASSERT_EQ(i, iread);
        }
        ASSERT_TRUE(!fr.HasNext());
    }
    ASSERT_TRUE(file.empty());
    ASSERT_EQ(0u, file.num_items());
}

TEST_F(File, RandomGetIndexOf) {
    static constexpr size_t size = 500;

    std::minstd_rand0 rng(0);

    // Create test file.
    data::File file(block_pool_, 0, /* dia_id */ 0);

    data::File::Writer fw = file.GetWriter(53);

    for (size_t i = 0; i < size; i++) {
        fw.Put(size - i - 1);
    }

    fw.Close();

    ASSERT_EQ(size, file.num_items());

    for (size_t i = 0; i < 100; i++) {
        size_t val = rng() % size;

        size_t idx = file.GetIndexOf(val, 0, std::greater<size_t>());
        ASSERT_EQ(val, file.GetItemAt<size_t>(idx));
    }
}

TEST_F(File, TieGetIndexOf) {
    const size_t size = 500;

    // Create test file.
    data::File file(block_pool_, 0, /* dia_id */ 0);

    data::File::Writer fw = file.GetWriter(53);

    for (size_t i = 0; i < size; i++) {
        fw.Put(i);
    }

    fw.Close();

    for (size_t i = 0; i < size; i++) {
        size_t idx = file.GetIndexOf(i, i, std::less<size_t>());

        ASSERT_EQ(idx, i);
    }
}

TEST_F(File, TieGetIndexOfWithDuplicates) {
    const size_t size = 500;

    std::minstd_rand0 rng(0);

    // Create test file.
    data::File file(block_pool_, 0, /* dia_id */ 0);

    data::File::Writer fw = file.GetWriter(53);

    for (size_t i = 0; i < size; i++) {
        fw.Put(i / 4);
    }

    fw.Close();

    ASSERT_EQ(size, file.num_items());

    for (size_t i = 0; i < size; i++) {
        if (i % 4 == 0) {
            size_t val = i / 4;
            size_t idxL = file.GetIndexOf(val, 0, std::less<size_t>());
            size_t idxH = file.GetIndexOf(val, size * 2, std::less<size_t>());
            size_t idxE = file.GetIndexOf(val, val, std::less<size_t>());

            ASSERT_EQ(val * 4, idxL);
            ASSERT_EQ(idxE, idxL);
            ASSERT_EQ(val * 4 + 4, idxH);
            ASSERT_EQ(val, file.GetItemAt<size_t>(idxL));
        }
        size_t val = i;
        size_t idxM = file.GetIndexOf(val / 4, val, std::less<size_t>());
        ASSERT_EQ(idxM, val);
    }
}

TEST_F(File, SeekReadSlicesOfFiles) {
    static constexpr bool debug = false;

    // construct a small-block File with lots of items.
    data::File file(block_pool_, 0, /* dia_id */ 0);

    // yes, this is a prime number as block size. -tb
    data::File::Writer fw = file.GetWriter(/* block_size */ 53);
    for (size_t i = 0; i < 1000; ++i) {
        fw.Put(i);
    }
    fw.Close();

    ASSERT_EQ(1000u, file.num_items());

    // read complete File
    data::File::KeepReader fr = file.GetKeepReader();
    for (size_t i = 0; i < 1000; ++i) {
        ASSERT_TRUE(fr.HasNext());
        ASSERT_EQ(i, fr.Next<size_t>());
    }
    ASSERT_FALSE(fr.HasNext());

    // read items 95-144
    auto check_range =
        [&](size_t begin, size_t end, bool at_end = false) {
            LOG << "Test range [" << begin << "," << end << ")";

            // seek in File to begin.
            data::File::KeepReader fr = file.GetReaderAt<size_t>(begin);

            // read a few items
            if (end - begin > 5 && !at_end) {
                for (size_t i = 0; i < 5; ++i) {
                    ASSERT_TRUE(fr.HasNext());
                    ASSERT_EQ(begin, fr.Next<size_t>());
                    ++begin;
                }
            }

            LOG << "GetReaderAt() done";

            // read the items [begin,end)
            {
                std::vector<data::Block> blocks
                    = fr.GetItemBatch<size_t>(end - begin);

                LOG << "GetItemBatch -> " << blocks.size() << " blocks";

                for (data::Block& b : blocks)
                    b.PinWait(0);

                data::BlockQueue queue(block_pool_, 0, /* dia_id */ 0);

                for (data::Block& b : blocks) {
                    queue.AppendPinnedBlock(
                        b.PinWait(0), /* is_last_block */ false);
                }
                queue.Close();

                data::BlockQueue::ConsumeReader qr = queue.GetConsumeReader(0);

                for (size_t i = begin; i < end; ++i) {
                    ASSERT_TRUE(qr.HasNext());
                    sLOG << "index" << i;
                    ASSERT_EQ(i, qr.Next<size_t>());
                }
                ASSERT_FALSE(qr.HasNext());
            }

            if (at_end) return;

            sLOG << "read more";
            static constexpr size_t more = 100;

            // read the items [end, end + more)
            {
                std::vector<data::Block> blocks
                    = fr.GetItemBatch<size_t>(more);

                data::BlockQueue queue(block_pool_, 0, /* dia_id */ 0);

                for (data::Block& b : blocks)
                    queue.AppendPinnedBlock(
                        b.PinWait(0), /* is_last_block */ false);
                queue.Close();

                data::BlockQueue::ConsumeReader qr = queue.GetConsumeReader(0);

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

    // some special cases: beginning, zero ranges, end.
    check_range(0, 0);
    check_range(0, 1);
    check_range(1, 2);
    check_range(100, 100);
    check_range(990, 1000, true);
    check_range(1000, 1000, true);
}

#if 0
//! A derivative of File which only contains a limited amount of Blocks
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable:4250)
#endif
class BoundedFile : public virtual data::BoundedBlockSink,
                    public virtual data::File
{
public:
    //! constructor with reference to BlockPool
    BoundedFile(data::BlockPool& block_pool, size_t local_worker_id,
                size_t dia_id, size_t max_size)
        : BlockSink(block_pool, local_worker_id),
          BoundedBlockSink(block_pool, local_worker_id, max_size),
          File(block_pool, local_worker_id, dia_id)
    { }

    static constexpr bool allocate_can_fail_ = true;
};
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

TEST_F(File, BoundedFilePutIntegerUntilFull) {

    // construct Partition with very small blocks for testing
    BoundedFile file(block_pool_, 0, /* dia_id */ 0, 32 * 64);

    try {
        data::BlockWriter<BoundedFile> bw(&file, 64);
        for (size_t i = 0; i != 1024000; ++i) {
            bw.Put(123456u + i);
        }
        FAIL();
    }
    catch (data::FullException&) {
        // good: we got the exception
    }

    ASSERT_EQ(file.max_size()
              / (sizeof(size_t)
                 + (data::BlockWriter<BoundedFile>::self_verify ? sizeof(size_t) : 0)),
              file.num_items());
}
#endif

// forced instantiation
template class data::BlockReader<data::KeepFileBlockSource>;
template class data::BlockReader<data::ConsumeFileBlockSource>;

// fixed size serialization test
static_assert(data::Serialization<data::File::Writer, int>
              ::is_fixed_size == true, "");
static_assert(data::Serialization<data::File::Writer, int>
              ::fixed_size == sizeof(int), "");

static_assert(data::Serialization<data::File::Writer, std::string>
              ::is_fixed_size == false, "");

static_assert(data::Serialization<data::File::Writer, std::pair<int, short> >
              ::is_fixed_size == true, "");
static_assert(data::Serialization<data::File::Writer, std::pair<int, short> >
              ::fixed_size == sizeof(int) + sizeof(short), "");

static_assert(data::Serialization<data::File::Writer, std::pair<int, std::string> >
              ::is_fixed_size == false, "");

/******************************************************************************/
