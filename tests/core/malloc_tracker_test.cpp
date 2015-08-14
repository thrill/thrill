/*******************************************************************************
 * tests/core/malloc_tracker_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/core/malloc_tracker.hpp>
#include <gtest/gtest.h>

using namespace c7a;

TEST(MallocTracker, Test1) {

    size_t curr = core::malloc_tracker_current();

    void* a = malloc(1024);

    size_t curr2 = core::malloc_tracker_current();
    ASSERT_GE(curr2, curr + 1024);

    free(a);

    ASSERT_LE(curr, curr2);
}

/******************************************************************************/
