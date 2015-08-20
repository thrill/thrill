/*******************************************************************************
 * tests/common/thread_pool_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/thread_pool.hpp>
#include <gtest/gtest.h>

#include <numeric>
#include <string>
#include <vector>

using namespace c7a::common;

TEST(ThreadPool1, LoopUntilEmpty) {
    size_t job_num = 256;

    std::vector<unsigned> result1(job_num, 0);
    std::vector<unsigned> result2(job_num, 0);

    ThreadPool pool(8);

    for (size_t r = 0; r != 16; ++r) {

        for (size_t i = 0; i != job_num; ++i) {
            pool.Enqueue(
                [i, &result1, &result2, &pool]() {
                    // set flag
                    result1[i] = 1 + i;

                    // enqueue more work: how to call this lambda again?
                    pool.Enqueue(
                        [i, &result2]() {
                            result2[i] = 2 + i;
                        });
                });
        }

        pool.LoopUntilEmpty();
    }

    // check that the threads have run
    for (size_t i = 0; i != job_num; ++i) {
        ASSERT_EQ(result1[i], 1 + i);
        ASSERT_EQ(result2[i], 2 + i);
    }
}

// obfuscated gtest magic to run test with two parameters
class ThreadPool2 : public::testing::TestWithParam<int>
{ };

TEST_P(ThreadPool2, LoopUntilTerminate) {
    static const bool debug = false;
    size_t job_num = 256;

    std::vector<int> result1(job_num, 0);
    std::vector<int> result2(job_num, 0);

    std::chrono::milliseconds sleep_time(GetParam());
    LOG << "sleep_time: " << GetParam();

    ThreadPool pool(8);

    for (size_t i = 0; i != job_num; ++i) {
        pool.Enqueue(
            [i, &result1, &result2, &pool, &sleep_time]() {
                // set flag
                result1[i] = 1;
                std::this_thread::sleep_for(sleep_time);

                // enqueue more work: how to call this lambda again?
                pool.Enqueue(
                    [i, &result2, &sleep_time]() {
                        result2[i] = 1;
                        std::this_thread::sleep_for(sleep_time);
                    });
            });
    }

    StatsTimer<true> timer1(true);

    // start thread which will stop the thread pool (if we would enqueue this as
    // job, it would be no different from the first test).
    std::thread stopper_thr = std::thread(
        [&pool]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            LOG << "Terminate now.";
            pool.Terminate();
        });

    pool.LoopUntilTerminate();
    LOG << "ThreadPool loop exited.";

    stopper_thr.join();
    LOG << "Stopper thread joined.";

    // check that it didn't terminate immediately.
    timer1.Stop();
    ASSERT_GT(timer1.Milliseconds(), 90);

    // check result: count number of flags set.
    size_t sum = std::accumulate(result1.begin(), result1.end(), 0u);
    sum += std::accumulate(result2.begin(), result2.end(), 0u);
    ASSERT_EQ(sum, pool.done());

    LOG << "Jobs done: " << sum << " vs maximum " << job_num * 2;
}

INSTANTIATE_TEST_CASE_P(
    ThreadPoolTerminate, ThreadPool2, ::testing::Values(1, 10));

/******************************************************************************/
