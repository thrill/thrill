/*******************************************************************************
 * tests/data/block_pool_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/data/block.hpp>
#include <thrill/data/block_pool.hpp>

#include <string>

using namespace thrill;

struct BlockPoolTest : public::testing::Test {
    data::BlockPool block_pool_;
};

TEST_F(BlockPoolTest, AllocateByteBlock) {
    data::PinnedByteBlockPtr block = block_pool_.AllocateByteBlock(8, 0);
}

TEST_F(BlockPoolTest, AllocatePinnedBlocks) {
    data::PinnedByteBlockPtr block = block_pool_.AllocateByteBlock(8, 0);
    data::PinnedBlock pblock(std::move(block), 0, 0, 0, 0);
    ASSERT_EQ(1u, block_pool_.total_blocks());
    ASSERT_EQ(8u, block_pool_.total_bytes());
    data::PinnedByteBlockPtr block2 = block_pool_.AllocateByteBlock(2, 0);
    data::PinnedBlock pblock2(std::move(block2), 0, 0, 0, 0);
    ASSERT_EQ(2u, block_pool_.total_blocks());
    ASSERT_EQ(10u, block_pool_.total_bytes());
}

TEST_F(BlockPoolTest, BlocksOutOfScopeReduceBlockCount) {
    {
        data::PinnedByteBlockPtr block = block_pool_.AllocateByteBlock(8, 0);
        data::PinnedBlock pblock(std::move(block), 0, 0, 0, 0);
    }
    ASSERT_EQ(0u, block_pool_.total_blocks());
}

TEST_F(BlockPoolTest, AllocatedBlocksHaveRefCountOne) {
    data::PinnedByteBlockPtr block = block_pool_.AllocateByteBlock(8, 0);
    data::PinnedBlock pblock(std::move(block), 0, 0, 0, 0);
    ASSERT_EQ(1u, pblock.byte_block()->reference_count());
}

TEST_F(BlockPoolTest, CopiedBlocksHaveRefCountOne) {
    data::PinnedByteBlockPtr block = block_pool_.AllocateByteBlock(8, 0);
    data::PinnedBlock pblock(std::move(block), 0, 0, 0, 0);
    ASSERT_FALSE(block.valid());

    data::PinnedBlock pblock_copy = pblock;
    ASSERT_EQ(2u, pblock.byte_block()->pin_count(0));
}

TEST_F(BlockPoolTest, PinnedBlock) {
    data::ByteBlockPtr bbp; /* unpinned */
    data::Block unpinned_block;
    {
        // allocate ByteBlock, construct PinnedBlock, and release pin.
        data::PinnedByteBlockPtr byte_block = block_pool_.AllocateByteBlock(8, 0);
        bbp = byte_block;
        data::PinnedBlock pinned_block(std::move(byte_block), 0, 0, 0, 0);
        ASSERT_EQ(1u, bbp->pin_count(0));
        unpinned_block = pinned_block;
        ASSERT_EQ(1u, bbp->pin_count(0));
    }
    ASSERT_EQ(0u, bbp->pin_count(0));
    {
        // refetch Pin on the ByteBlock
        std::future<data::PinnedBlock> pin = unpinned_block.Pin(0);
        pin.wait();
        data::PinnedBlock pinned_block = pin.get();
        ASSERT_EQ(1u, bbp->pin_count(0));
    }
    ASSERT_EQ(0u, bbp->pin_count(0));
}

/******************************************************************************/
