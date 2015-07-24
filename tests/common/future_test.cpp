/*******************************************************************************
 * tests/common/future_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/future.hpp>
#include <c7a/common/thread_pool.hpp>
#include <gtest/gtest.h>

#include <string>

using namespace c7a::common;
using namespace std::literals;

struct FutureTest : public::testing::Test {
    FutureTest() : pool(2) { }
    ThreadPool pool;

    // can not put f or result in the struct because it cannot be captured then.
};

TEST_F(FutureTest, GetReturnsCorrectValue) {
    Future<int> f;

    pool.Enqueue([&f]() {
                     int result = f.Wait();
                     ASSERT_EQ(42, result);
                 });

    pool.Enqueue([&f]() {
                     f.Callback(42);
                 });

    pool.LoopUntilEmpty();
}

TEST_F(FutureTest, IsFinishedIsSetAfterCallback) {
    Future<int> f;

    pool.Enqueue([&f]() {
                     std::this_thread::sleep_for(100ms);
                     int result = f.Wait();
                     ASSERT_EQ(42, result);
                 });

    pool.Enqueue([&f]() {
                     ASSERT_FALSE(f.is_finished());
                     f.Callback(42);

                     // let other thread run, but that one will wait 100ms
                     std::this_thread::sleep_for(10ns);
                     ASSERT_FALSE(f.is_finished());

                     // this should be after the the other thread called Get
                     std::this_thread::sleep_for(200ms);
                     ASSERT_TRUE(f.is_finished());
                 });

    pool.LoopUntilEmpty();
}

/******************************************************************************/
