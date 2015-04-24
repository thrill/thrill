#include "gtest/gtest.h"
#include "c7a/data/block_iterator.hpp"

using namespace c7a::data;

struct TestBlockIterator : public ::testing::Test {
    TestBlockIterator() 
        : emptyIt(emptyData.begin(), emptyData.end())
        , it     (data.begin(),      data.end()) { }

    std::vector<std::string> emptyData = {};
    std::vector<std::string> data = { "foo", "bar" };
    BlockIterator<int> emptyIt;
    BlockIterator<std::string> it;
};

TEST_F(TestBlockIterator, EmptyArrayHasNotNext) {
    ASSERT_FALSE(emptyIt.has_next());
}

TEST_F(TestBlockIterator, IterateOverStrings) {
    ASSERT_EQ("foo", it.next());
    ASSERT_EQ("bar", it.next());
}

TEST_F(TestBlockIterator, HasNextReturnsFalseAtTheEnd) {
    (void) it.next();
    (void) it.next();
    ASSERT_FALSE(it.has_next());
}

TEST_F(TestBlockIterator, HasNextReturnsTrueInTheMiddle) {
    (void) it.next();
    ASSERT_TRUE(it.has_next());
}
