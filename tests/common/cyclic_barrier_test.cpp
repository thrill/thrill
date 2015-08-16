/*******************************************************************************
 * tests/common/cyclic_barrier_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/cyclic_barrier.hpp>
#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <ctime>
#include <thread>
#include <vector>

using namespace c7a::common;

static void TestWaitFor(int count, int slowThread = -1) {

    // TODO(ej): use new C++11 random things instead.
    srand(time(nullptr));
    int maxWaitTime = 100000;

    Barrier barrier(count);
    // Need to use atomic here, since setting a bool might not be atomic.
    std::vector<std::atomic<bool> > flags(count);
    std::vector<std::thread> threads(count);

    for (int i = 0; i < count; i++) {
        flags[i] = false;
    }

    for (int i = 0; i < count; i++) {
        threads[i] = std::thread(
            [maxWaitTime, count, slowThread, &barrier, &flags, i] {

                if (slowThread == -1) {
                    usleep(rand() % maxWaitTime);
                }
                else if (i == slowThread) {
                    usleep(rand() % maxWaitTime);
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

TEST(Barrier, TestWaitForSingleThread) {
    int count = 8;
    for (int i = 0; i < count; i++) {
        TestWaitFor(count, i);
    }
}

TEST(Barrier, TestWaitFor) {
    TestWaitFor(32);
}
/******************************************************************************/
