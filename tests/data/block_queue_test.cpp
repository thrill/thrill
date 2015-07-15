/*******************************************************************************
 * tests/data/block_queue_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <tobias.sturm@student.kit.edu>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "c7a/data/block_queue.hpp"

using namespace c7a::data;

struct BlockQueueTest : public::testing::Test {
    BlockQueue<16> q;
};

TEST_F(BlockQueueTest, FreshQueueIsNotClosed) {
    ASSERT_FALSE(q.closed());
}

TEST_F(BlockQueueTest, QueueCanBeClosed) {
    q.Close();
    ASSERT_TRUE(q.closed());
}

TEST_F(BlockQueueTest, FreshQueueIsEmpty) {
    ASSERT_TRUE(q.empty());
}

TEST_F(BlockQueueTest, QueueNonEmptyAfterAppend) {
    std::shared_ptr<Block<16> > block;
    q.Append(block, 0, 0, 0);
    ASSERT_FALSE(q.empty());
}

TEST_F(BlockQueueTest, BlockWriterToQueue) {
    BlockWriter<Block<16>, BlockQueue<16> > bw(q);
    bw(int(42));
    bw(std::string("hello there BlockQueue"));
    bw.Close();
    ASSERT_FALSE(q.empty());
    ASSERT_EQ(q.size(), 2u);
}

/******************************************************************************/
