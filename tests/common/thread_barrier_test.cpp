/*******************************************************************************
 * tests/common/thread_barrier_test.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/thread_barrier.hpp>

#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <ctime>
#include <random>
#include <thread>
#include <vector>

using namespace thrill::common;

static void TestWaitFor(int count, int slowThread = -1) {

    int maxWaitTime = 100000;

    ThreadBarrier barrier(count);
    // Need to use atomic here, since setting a bool might not be atomic.
    std::vector<std::atomic<bool> > flags(count);
    std::vector<std::thread> threads(count);

    for (int i = 0; i < count; i++) {
        flags[i] = false;
    }

    for (int i = 0; i < count; i++) {
        threads[i] = std::thread(
            [maxWaitTime, count, slowThread, &barrier, &flags, i] {
                std::minstd_rand0 rng(i);
                rng();

                if (slowThread == -1) {
                    usleep(rng() % maxWaitTime);
                }
                else if (i == slowThread) {
                    usleep(rng() % maxWaitTime);
                }

                flags[i] = true;

                barrier.Await();

                for (int j = 0; j < count; j++) {
                    ASSERT_EQ(flags[j], true);
                }
            });
    }

    for (int i = 0; i < count; i++) {
        threads[i].join();
    }
}

TEST(ThreadBarrier, TestWaitForSingleThread) {
    int count = 8;
    for (int i = 0; i < count; i++) {
        TestWaitFor(count, i);
    }
}

TEST(ThreadBarrier, TestWaitFor) {
    TestWaitFor(32);
}

/******************************************************************************/
