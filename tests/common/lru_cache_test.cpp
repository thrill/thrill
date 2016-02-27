/*******************************************************************************
 * tests/common/lru_cache_test.cpp
 *
 * Borrowed from https://github.com/lamerman/cpp-lru-cache by Alexander
 * Ponomarev under BSD license and modified for Thrill's block pool.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2013 Alexander Ponomarev
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/lru_cache.hpp>

using namespace thrill;

static constexpr int kNumOfTest2Records = 100;
static constexpr int kTest2CacheCapacity = 50;

TEST(LruCacheTest, SimplePut) {
    common::LruCache<int, int> cache;
    cache.put(7, 777);
    EXPECT_TRUE(cache.exists(7));
    EXPECT_EQ(777, cache.get(7));
    EXPECT_EQ(1, cache.size());
}

TEST(LruCacheTest, MissingValue) {
    common::LruCache<int, int> cache;
    EXPECT_THROW(cache.get(7), std::range_error);
}

TEST(LruCacheTest1, KeepAllValuesWithinCapacity) {
    common::LruCache<int, int> cache;

    for (int i = 0; i < kNumOfTest2Records; ++i) {
        cache.put(i, i);

        while (cache.size() > kTest2CacheCapacity)
            cache.pop();
    }

    for (int i = 0; i < kNumOfTest2Records - kTest2CacheCapacity; ++i) {
        EXPECT_FALSE(cache.exists(i));
    }

    for (int i = kNumOfTest2Records - kTest2CacheCapacity; i < kNumOfTest2Records; ++i) {
        EXPECT_TRUE(cache.exists(i));
        EXPECT_EQ(i, cache.get(i));
    }

    size_t size = cache.size();
    EXPECT_EQ(kTest2CacheCapacity, size);
}

/******************************************************************************/
