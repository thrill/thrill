/*******************************************************************************
 * tests/common/cmdline_parser_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/cmdline_parser.hpp>

#include <sstream>
#include <string>

using namespace c7a::common;

TEST(CmdlineParser, Test1) {
    int a_int = 0;
    std::string a_str;

    CmdlineParser cp;
    cp.AddInt('i', "int", "<N>", a_int, "an integer");
    cp.AddString('f', "filename", "<F>", a_str, "a filename");

    cp.SetDescription("Command Line Parser Test");
    cp.SetAuthor("Timo Bingmann <tb@panthema.net>");

    // good command line
    const char* cmdline1[] =
    { "test", "-i", "42", "-f", "somefile", nullptr };

    std::ostringstream os1;
    ASSERT_TRUE(cp.Process(5, cmdline1, os1));

    ASSERT_EQ(a_int, 42);
    ASSERT_EQ(a_str, "somefile");

    // bad command line
    const char* cmdline2[] =
    { "test", "-i", "dd", "-f", "somefile", nullptr };

    std::ostringstream os2;
    ASSERT_FALSE(cp.Process(5, cmdline2, os2));
}

/******************************************************************************/
