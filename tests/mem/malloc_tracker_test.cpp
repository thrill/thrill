/*******************************************************************************
 * tests/mem/malloc_tracker_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/mem/malloc_tracker.hpp>
#include <gtest/gtest.h>

using namespace c7a;

TEST(MallocTracker, Test1) {

    size_t curr = mem::malloc_tracker_current();
    ASSERT_GE(curr, 0);

    char* a = nullptr;
    {
        a = reinterpret_cast<char*>(malloc(1024));
        // dear compiler, please don't optimize away completely!
        a[0] = 0;
    }

    size_t curr2 = mem::malloc_tracker_current();

    volatile char* av = a;
    ASSERT_GE(curr2 + av[0], curr + 1024);

    free(a);

    ASSERT_LE(curr, curr2);
}

/******************************************************************************/
