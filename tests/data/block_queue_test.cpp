/*******************************************************************************
 * tests/data/block_queue_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/thread_pool.hpp>
#include <thrill/data/block_queue.hpp>
#include <thrill/data/concat_block_source.hpp>

#include <string>

using namespace c7a;

using MyQueue = data::BlockQueue;
using MyBlockSource = MyQueue::BlockSource;
using ConcatBlockSource = data::ConcatBlockSource<MyBlockSource>;

struct BlockQueue : public::testing::Test {
    MyQueue q;
};

TEST_F(BlockQueue, FreshQueueIsNotClosed) {
    ASSERT_FALSE(q.write_closed());
}

TEST_F(BlockQueue, QueueCanBeClosed) {
    q.Close();
    ASSERT_TRUE(q.write_closed());
}

TEST_F(BlockQueue, FreshQueueIsEmpty) {
    ASSERT_TRUE(q.empty());
}

TEST_F(BlockQueue, QueueNonEmptyAfterAppend) {
    data::ByteBlockPtr bytes = data::ByteBlock::Allocate(16);
    q.AppendBlock(data::Block(bytes, 0, 0, 0, 0));
    ASSERT_FALSE(q.empty());
}

TEST_F(BlockQueue, BlockWriterToQueue) {
    MyQueue::Writer bw = q.GetWriter(16);
    bw(static_cast<int>(42));
    bw(std::string("hello there BlockQueue"));
    bw.Close();
    ASSERT_FALSE(q.empty());
    // two real block and one termination sentinel. with verify one more.
    ASSERT_EQ(2u + (MyQueue::Writer::self_verify ? 1 : 0), q.size());
}

TEST_F(BlockQueue, ThreadedParallelBlockWriterAndBlockReader) {
    common::ThreadPool pool(2);
    MyQueue q;

    pool.Enqueue(
        [&q]() {
            MyQueue::Writer bw = q.GetWriter(16);
            bw(static_cast<int>(42));
            bw(std::string("hello there BlockQueue"));
        });

    pool.Enqueue(
        [&q]() {
            MyQueue::Reader br = q.GetReader();

            ASSERT_TRUE(br.HasNext());
            int i1 = br.Next<int>();
            ASSERT_EQ(42, i1);

            ASSERT_TRUE(br.HasNext());
            std::string i2 = br.Next<std::string>();
            ASSERT_EQ("hello there BlockQueue", i2);

            ASSERT_FALSE(br.HasNext());
        });

    pool.LoopUntilEmpty();
}

TEST_F(BlockQueue, OrderedMultiQueue_Multithreaded) {
    using namespace std::literals;
    common::ThreadPool pool(3);
    MyQueue q2;

    auto writer1 = q.GetWriter(16);
    auto writer2 = q2.GetWriter(16);

    pool.Enqueue([&writer1]() {
                     writer1(std::string("1.1"));
                     std::this_thread::sleep_for(25ms);
                     writer1(std::string("1.2"));
                     writer1.Close();
                 });
    pool.Enqueue([&writer2]() {
                     writer2(std::string("2.1"));
                     writer2.Flush();
                     writer2(std::string("2.2"));
                     writer2.Close();
                 });
    pool.Enqueue([this, &q2]() {
                     auto reader = data::BlockReader<ConcatBlockSource>(
                         ConcatBlockSource(
                             { MyBlockSource(q), MyBlockSource(q2) }));
                     ASSERT_EQ("1.1", reader.Next<std::string>());
                     ASSERT_EQ("1.2", reader.Next<std::string>());
                     ASSERT_EQ("2.1", reader.Next<std::string>());
                     ASSERT_EQ("2.2", reader.Next<std::string>());
                 });
    pool.LoopUntilEmpty();
}

TEST_F(BlockQueue, ThreadedParallelBlockWriterAndDynBlockReader) {
    common::ThreadPool pool(2);
    MyQueue q;

    pool.Enqueue(
        [&q]() {
            MyQueue::Writer bw = q.GetWriter(16);
            bw(static_cast<int>(42));
            bw(std::string("hello there BlockQueue"));
        });

    pool.Enqueue(
        [&q]() {
            MyQueue::DynReader br = q.GetDynReader();

            ASSERT_TRUE(br.HasNext());
            int i1 = br.Next<int>();
            ASSERT_EQ(42, i1);

            ASSERT_TRUE(br.HasNext());
            std::string i2 = br.Next<std::string>();
            ASSERT_EQ("hello there BlockQueue", i2);

            ASSERT_FALSE(br.HasNext());
        });

    pool.LoopUntilEmpty();
}

/******************************************************************************/
