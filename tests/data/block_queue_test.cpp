/*******************************************************************************
 * tests/data/block_queue_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "c7a/data/block_queue.hpp"

using namespace c7a::data;

struct BlockQueueTest : public::testing::Test { };

TEST_F(BlockQueueTest, FreshQueueIsNotClosed) {
    BlockQueue<16> q;
    ASSERT_FALSE(q.closed());
}

TEST_F(BlockQueueTest, QueueCanBeClosed) {
    BlockQueue<16> q;
    q.Close();
    ASSERT_TRUE(q.closed());
}

TEST_F(BlockQueueTest, FreshQueueIsEmpty) {
    BlockQueue<16> q;
    ASSERT_TRUE(q.empty());
}

TEST_F(BlockQueueTest, QueueNonEmptyAfterAppend) {
    BlockQueue<16> q;
    std::shared_ptr<Block<16> > block;
    q.Append(block, 0, 0, 0);
    ASSERT_FALSE(q.empty());
}

TEST_F(BlockQueueTest, BlockWriterToQueue) {
    BlockQueue<16> q;
    BlockWriter<Block<16>, BlockQueue<16> > bw(q);
    bw(int(42));
    bw(std::string("hello there BlockQueue"));
    bw.Close();
    ASSERT_FALSE(q.empty());
    ASSERT_EQ(q.size(), 2u);
}

/******************************************************************************/
