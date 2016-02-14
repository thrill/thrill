/*******************************************************************************
 * tests/common/string_view_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/string_view.hpp>

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>

#include <string>

using thrill::common::StringView;

TEST(StringViewTest, ConstructEmpty) {
    StringView empty;
    ASSERT_EQ(empty.size(), 0);
}

TEST(StringViewTest, AssignAndCompare) {

    std::string input = "This is a string which does things and is our input.";
    std::string input2 = "is a string1";

    StringView other_str;
    StringView fast_str = StringView(&input[5], 11); //"is a string"
    ASSERT_EQ(fast_str.size(), 11);

    std::string cmp = "is a string";
    ASSERT_TRUE(fast_str == cmp);

    std::string different = "is another string";
    ASSERT_FALSE(fast_str == different);

    std::string subset = "is a strin";
    ASSERT_FALSE(fast_str == subset);

    other_str = StringView(&input[6], 11);             //"s a string "
    ASSERT_FALSE(fast_str == other_str);
    ASSERT_TRUE(fast_str != other_str);
    StringView equal_str = StringView(&input2[0], 11); //"is a string"
    ASSERT_TRUE(fast_str == equal_str);
    ASSERT_FALSE(fast_str != equal_str);
}

/******************************************************************************/
