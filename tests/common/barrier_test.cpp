/*******************************************************************************
 * tests/common/barrier_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/cyclic_barrier.hpp>

#include <string>
#include <numeric>
#include <thread>
#include <atomic>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "gtest/gtest.h"

using namespace c7a::common;

static void TestWaitFor(int count, int slowThread = -1) {

    srand(time(NULL));
    int maxWaitTime = 100000;

    Barrier barrier(count);
    //Need to use atomic here, since setting a bool might not be atomic.
    std::vector<std::atomic<bool> > flags(count);
    std::vector<std::thread*> threads(count);

    for (int i = 0; i < count; i++) {
        flags[i] = false;
    }

    for (int i = 0; i < count; i++) {
        threads[i] = new std::thread([maxWaitTime, count, slowThread, &barrier, &flags, i] {

                                         if (slowThread == -1) {
                                             usleep(rand() % maxWaitTime);
                                         }
                                         else if (i == slowThread) {
                                             usleep(rand() % maxWaitTime);
                                         }

                                         flags[i] = true;

                                         barrier.await();

                                         for (int j = 0; j < count; j++) {
                                             ASSERT_EQ(flags[j], true);
                                         }
                                     });
    }

    for (int i = 0; i < count; i++) {
        threads[i]->join();
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
