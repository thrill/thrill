/*******************************************************************************
 * tests/data/manager_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "c7a/data/block_queue.hpp"

using namespace c7a::data;

struct BlockQueueTest : public::testing::Test {
    std::shared_ptr<Block<1> > block;
    size_t                     block_used = 0;
    size_t                     nitems = 0;
    size_t                     first = 0;
};

TEST_F(BlockQueueTest, FreshQueueIsNotClosed) {
    BlockQueue<1> q;
    ASSERT_FALSE(q.closed());
}

TEST_F(BlockQueueTest, QueueCanBeClosed) {
    BlockQueue<1> q;
    q.Close();
    ASSERT_TRUE(q.closed());
}

TEST_F(BlockQueueTest, FreshQueueIsEmpty) {
    BlockQueue<1> q;
    ASSERT_TRUE(q.empty());
}

TEST_F(BlockQueueTest, QueueNonEmptyAfterAppend) {
    BlockQueue<1> q;
    q.Append(block, block_used, nitems, first);
    ASSERT_FALSE(q.empty());
}

/******************************************************************************/
