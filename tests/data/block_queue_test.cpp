/*******************************************************************************
 * tests/data/block_queue_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <tobias.sturm@student.kit.edu>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include <c7a/data/block_queue.hpp>
#include <c7a/common/thread_pool.hpp>

using namespace c7a;

struct BlockQueueTest : public::testing::Test {
    data::BlockQueue<16> q;
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
    std::shared_ptr<data::Block<16> > block
        = std::make_shared<data::Block<16> >();
    q.Append(block, 0, 0, 0);
    ASSERT_FALSE(q.empty());
}

TEST_F(BlockQueueTest, BlockWriterToQueue) {
    data::BlockWriter<data::Block<16>, data::BlockQueue<16> > bw(q);
    bw(int(42));
    bw(std::string("hello there BlockQueue"));
    bw.Close();
    ASSERT_FALSE(q.empty());
    ASSERT_EQ(q.size(), 2u);
}

TEST_F(BlockQueueTest, ThreadedParallelBlockWriterAndBlockReader) {
    common::ThreadPool pool(2);
    data::BlockQueue<16> q;

    pool.Enqueue(
        [&q]() {
            data::BlockWriter<data::Block<16>, data::BlockQueue<16> > bw(q);
            bw(int(42));
            bw(std::string("hello there BlockQueue"));
        });

    pool.Enqueue([&q]() { });

    pool.LoopUntilEmpty();
}

/******************************************************************************/
