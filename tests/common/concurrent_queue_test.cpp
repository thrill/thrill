/*******************************************************************************
 * tests/common/concurrent_queue_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/concurrent_queue.hpp>
#include <tlx/thread_pool.hpp>

#include <atomic>
#include <memory>

using namespace thrill::common;

TEST(ConcurrentQueue, ParallelPushPopAscIntegerAndCalculateTotalSum) {
    tlx::ThreadPool pool(8);

    ConcurrentQueue<size_t, std::allocator<size_t> > queue;
    std::atomic<size_t> count(0);
    std::atomic<size_t> total_sum(0);

    static constexpr size_t num_threads = 4;
    static constexpr size_t num_pushes = 10000;

    // have threads push items

    for (size_t i = 0; i != num_threads; ++i) {
        pool.enqueue([&queue]() {
                         for (size_t i = 0; i != num_pushes; ++i) {
                             queue.push(i);
                         }
                     });
    }

    // have threads try to pop items.

    for (size_t i = 0; i != num_threads; ++i) {
        pool.enqueue([&]() {
                         while (count != num_threads * num_pushes) {
                             size_t item;
                             while (queue.try_pop(item)) {
                                 total_sum += item;
                                 ++count;
                             }
                         }
                     });
    }

    pool.loop_until_empty();

    ASSERT_TRUE(queue.empty());
    ASSERT_EQ(count, num_threads * num_pushes);
    // check total sum, no item gets lost?
    ASSERT_EQ(total_sum, num_threads * num_pushes * (num_pushes - 1) / 2);
}

/******************************************************************************/
