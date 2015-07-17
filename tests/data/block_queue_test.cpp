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

using MyQueue = data::BlockQueue<16>;

struct BlockQueueTest : public::testing::Test {
    MyQueue q;
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
    q.Append(data::VirtualBlock<16>(block, 0, 0, 0));
    ASSERT_FALSE(q.empty());
}

TEST_F(BlockQueueTest, BlockWriterToQueue) {
    MyQueue::Writer bw = q.GetWriter();
    bw(int(42));
    bw(std::string("hello there BlockQueue"));
    bw.Close();
    ASSERT_FALSE(q.empty());
    // two real block and one termination sentinel. with verify one more.
    ASSERT_EQ(q.size(), 3u + (MyQueue::Writer::self_verify ? 1 : 0));
}

TEST_F(BlockQueueTest, ThreadedParallelBlockWriterAndBlockReader) {
    common::ThreadPool pool(2);
    MyQueue q;

    pool.Enqueue(
        [&q]() {
            MyQueue::Writer bw = q.GetWriter();
            bw(int(42));
            bw(std::string("hello there BlockQueue"));
        });

    pool.Enqueue(
        [&q]() {
            MyQueue::Reader br = q.GetReader();

            ASSERT_TRUE(br.HasNext());
            int i1 = br.Next<int>();
            ASSERT_EQ(i1, 42);

            ASSERT_TRUE(br.HasNext());
            std::string i2 = br.Next<std::string>();
            ASSERT_EQ(i2, "hello there BlockQueue");

            ASSERT_FALSE(br.HasNext());
        });

    pool.LoopUntilEmpty();
}

/******************************************************************************/
