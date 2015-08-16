/*******************************************************************************
 * tests/common/future_queue_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/future_queue.hpp>
#include <c7a/common/thread_pool.hpp>
#include <gtest/gtest.h>

using namespace c7a::common;
using namespace std::literals;

struct FutureQueueTest : public::testing::Test {
    FutureQueueTest() : pool(2) { }
    ThreadPool pool;

    // can not put f or result in the struct because it cannot be captured then.
};

TEST_F(FutureQueueTest, WaitIsTrueWhenDataWasPushed) {
    FutureQueue<int> fq;
    fq.Callback(42, false);
    ASSERT_TRUE(fq.Wait());
}

TEST_F(FutureQueueTest, WaitReturnsFalseIfClosed) {
    FutureQueue<int> fq;
    fq.Callback(0, true);
    ASSERT_FALSE(fq.Wait());
}

TEST_F(FutureQueueTest, IsClosedIsFalseWhenNotClosed) {
    FutureQueue<int> fq;
    fq.Callback(42, false);
    ASSERT_FALSE(fq.closed());
}

TEST_F(FutureQueueTest, IsClosedIsTrueWhenClosedDirectly) {
    FutureQueue<int> fq;
    fq.Callback(0, false);
    ASSERT_FALSE(fq.closed());
}

TEST_F(FutureQueueTest, NextReturnsElementsInCorrectOrder) {
    FutureQueue<int> fq;
    fq.Callback(1, false);
    fq.Callback(2, false);
    fq.Callback(3, false);
    ASSERT_EQ(1, fq.Next());
    ASSERT_EQ(2, fq.Next());
    ASSERT_EQ(3, fq.Next());
}

TEST_F(FutureQueueTest, WaitWaitsForDataAndReturnsTrue) {
    FutureQueue<int> fq;

    pool.Enqueue([&fq]() {
                     ASSERT_TRUE(fq.Wait());
                     ASSERT_EQ(1, fq.Next());
                 });
    pool.Enqueue([&fq]() {
                     fq.Callback(1, false);
                 });
    pool.LoopUntilEmpty();
}

TEST_F(FutureQueueTest, WaitWaitsForDataAndReturnsFalseIfQueueIsClosed) {
    FutureQueue<int> fq;

    pool.Enqueue([&fq]() {
                     ASSERT_FALSE(fq.Wait());
                 });
    pool.Enqueue([&fq]() {
                     fq.Callback(1, true);
                 });
    pool.LoopUntilEmpty();
}

TEST_F(FutureQueueTest, WaitForAllWaitsAndReturnsFalseIfQueueIsClosed) {
    FutureQueue<int> fq;

    pool.Enqueue([&fq]() {
                     ASSERT_FALSE(fq.WaitForAll());
                 });
    pool.Enqueue([&fq]() {
                     fq.Callback(1, true);
                 });
    pool.LoopUntilEmpty();
}

TEST_F(FutureQueueTest, WaitForAllWaitsForDataAndReturnsTrue) {
    FutureQueue<int> fq;

    pool.Enqueue([&fq]() {
                     ASSERT_TRUE(fq.WaitForAll());
                     ASSERT_EQ(1, fq.Next());
                     ASSERT_EQ(1337, fq.Next());
                     ASSERT_EQ(42, fq.Next());
                     ASSERT_FALSE(fq.WaitForAll());
                     ASSERT_TRUE(fq.closed());
                 });
    pool.Enqueue([&fq]() {
                     fq.Callback(1, false);
                     fq.Callback(1337, false);
                     fq.Callback(42, false);
                     fq.Callback(0, true);
                 });
    pool.LoopUntilEmpty();
}

/******************************************************************************/
