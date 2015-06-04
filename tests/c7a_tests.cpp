/*******************************************************************************
 * tests/c7a_tests.cpp
 *
 * Google Test main program which calls tests.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <string>

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

/******************************************************************************/
