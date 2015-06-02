/*******************************************************************************
 * tests/common/future_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/common/future.hpp>
#include <c7a/common/thread_pool.hpp>
#include <c7a/net/dispatcher.hpp>

#include <string>

#include "gtest/gtest.h"

using namespace c7a::common;
using namespace std::literals;

struct FutureTest : public::testing::Test {
    FutureTest() : pool(2) { }
    ThreadPool pool;

    //can not put f or result in the struct because it cannot be captured then.
};

TEST_F(FutureTest, GetReturnsCorrectValue) {
    Future<int> f;
    int result = 0;

    pool.Enqueue([&f, &result]() {
                     result = f.Get();
                     ASSERT_EQ(42, result);
                 });

    pool.Enqueue([&f]() {
                     f.GetCallback()(42);
                 });

    pool.LoopUntilEmpty();
}

TEST_F(FutureTest, GetReturnsAfterCallback) {
    Future<int> f;
    int result = 0;

    pool.Enqueue([&f, &result]() {
                     result = f.Get();
                 });

    pool.Enqueue([&f, &result]() {
                     ASSERT_EQ(0, result);
                     f.GetCallback()(42);
                     std::this_thread::sleep_for(10ns);
                     ASSERT_EQ(42, result);
                 });

    pool.LoopUntilEmpty();
}

TEST_F(FutureTest, IsFinishedIsSetAfterCallback) {
    Future<int> f;
    int result = 0;

    pool.Enqueue([&f, &result]() {
                     std::this_thread::sleep_for(100ms);
                     result = f.Get();
                 });

    pool.Enqueue([&f, &result]() {
                     ASSERT_FALSE(f.is_finished());
                     f.GetCallback()(42);

                     //let other thread run, but that one will wait 100ms
                     std::this_thread::sleep_for(10ns);
                     ASSERT_FALSE(f.is_finished());

                     //this should be after the the other thread called Get
                     std::this_thread::sleep_for(200ms);
                     ASSERT_TRUE(f.is_finished());
                 });

    pool.LoopUntilEmpty();
}

/******************************************************************************/
