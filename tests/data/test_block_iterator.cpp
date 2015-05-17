/*******************************************************************************
 * tests/data/test_block_iterator.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/data/block_iterator.hpp>

#include <vector>
#include <string>

#include "gtest/gtest.h"

using namespace c7a::data;

struct TestBlockIterator : public::testing::Test {
    TestBlockIterator()
        : emptyIt(emptyData),
          it(data) { }

    std::vector<std::string>   emptyData = { };
    std::vector<std::string>   data = { "foo", "bar" };
    BlockIterator<int>         emptyIt;
    BlockIterator<std::string> it;
};

TEST_F(TestBlockIterator, EmptyArrayHasNotNext) {
    ASSERT_FALSE(emptyIt.HasNext());
}

TEST_F(TestBlockIterator, IterateOverStrings) {
    ASSERT_EQ("foo", it.Next());
    ASSERT_EQ("bar", it.Next());
}

TEST_F(TestBlockIterator, HasNextReturnsFalseAtTheEnd) {
    (void)it.Next();
    (void)it.Next();
    ASSERT_FALSE(it.HasNext());
}

TEST_F(TestBlockIterator, HasNextReturnsTrueInTheMiddle) {
    (void)it.Next();
    ASSERT_TRUE(it.HasNext());
}

/******************************************************************************/
