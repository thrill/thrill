/*******************************************************************************
 * tests/mem/malloc_tracker_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/mem/malloc_tracker.hpp>

using namespace thrill;

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
