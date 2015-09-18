/*******************************************************************************
 * tests/common/fast_string_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/fast_string.hpp>
#include <thrill/common/logger.hpp>

#include <string>

using thrill::common::FastString;

TEST(FastStringTest, ConstructEmpty) {
	FastString empty;
	ASSERT_EQ(empty.Size(), 0);
}

TEST(FastStringTest, AssignAndCompare) {

	
	std::string input = "This is a string which does things and is our input.";
	std::string input2 = "is a string1";

	FastString other_str;
	FastString fast_str = FastString::Ref(&input[5], 11); //"is a string"
	ASSERT_EQ(fast_str.Size(), 11); 
	
	std::string cmp = "is a string";
	ASSERT_TRUE(fast_str == cmp);

	std::string different = "is another string"; 
	ASSERT_FALSE(fast_str == different);

	std::string subset = "is a strin";
	ASSERT_FALSE(fast_str == subset);

	other_str.Ref(&input[6], 11); //"s a string "
	ASSERT_FALSE(fast_str == other_str);
	ASSERT_TRUE(fast_str != other_str);
	FastString equal_str = FastString::Ref(&input2[0], 11); //"is a string"
	ASSERT_TRUE(fast_str == equal_str);
	ASSERT_FALSE(fast_str != equal_str);
}

TEST(FastStringTest, CopyFastString) {
	
	FastString str;
	{
		std::string input = "input string";
		FastString str2 = FastString::Ref(&input[0], 12);
		str = str2;
		FastString str3 = FastString::Copy(str2.Data(), str2.Size());
		std::string().swap(input);
		ASSERT_TRUE(str3 == "input string");
	}
	
	ASSERT_TRUE(str == "input string");
	ASSERT_EQ(str.Size(), 12);
}

TEST(FastStringTest, MoveFastString) {
	FastString str;
	std::string input = "input string";
	FastString str2 = FastString::Copy(&input[0], 12);
	str = std::move(str2);	
	std::string().swap(input);
	ASSERT_TRUE(str == "input string");
	ASSERT_EQ(str.Size(), 12);
}

/******************************************************************************/
