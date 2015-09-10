/*******************************************************************************
 * tests/data/block_pool_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/data/block_pool.hpp>

#include <string>

using namespace thrill;

struct BlockPoolTest : public::testing::Test {
    BlockPoolTest()
        : mem_manager_(nullptr, "mem"),
          ext_mem_manager_(nullptr, "ext"),
          block_pool_(&mem_manager_, &ext_mem_manager_) { }

    mem::Manager    mem_manager_;
    mem::Manager    ext_mem_manager_;
    data::BlockPool block_pool_;
};

TEST_F(BlockPoolTest, AllocateAccountsSizeInManager) {
    auto block = block_pool_.AllocateBlock(8);
    ASSERT_EQ(8u, mem_manager_.total());
    auto block2 = block_pool_.AllocateBlock(2);
    ASSERT_EQ(10u, mem_manager_.total());
}

TEST_F(BlockPoolTest, AllocateIncreasesBlockCountByOne) {
    auto block = block_pool_.AllocateBlock(8);
    ASSERT_EQ(1u, block_pool_.block_count());
}

TEST_F(BlockPoolTest, BlocksOutOfScopeReduceBlockCount) {
    {
        auto block = block_pool_.AllocateBlock(8);
    }
    ASSERT_EQ(0u, block_pool_.block_count());
}

TEST_F(BlockPoolTest, BlocksOutOfScopeAreAccountetInMemManager) {
    {
        auto block = block_pool_.AllocateBlock(8);
    }
    ASSERT_EQ(0u, mem_manager_.total());
}

TEST_F(BlockPoolTest, AllocatedBlocksHaveRefCountOne) {
    auto block = block_pool_.AllocateBlock(8);
    ASSERT_EQ(1u, block->reference_count());
}

TEST_F(BlockPoolTest, CopiedBlocksHaveRefCountOne) {
    auto block = block_pool_.AllocateBlock(8);
    auto copy = block;
    ASSERT_EQ(2u, block->reference_count());
}

/******************************************************************************/
