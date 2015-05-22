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
using namespace c7a::net;

struct TestBlockIterator : public::testing::Test {
    TestBlockIterator() :
        threeStrings({ "foo", "bar", "blub" }),
        oneString({ "." }),
        emptyBuffer(nullptr, 0),
        threeStringsBuffer(StringsToBuffer(threeStrings)),
        oneStringBuffer(StringsToBuffer(oneString)) { }

    BinaryBuffer StringsToBuffer(std::vector<std::string> strings) const {
        BinaryBufferBuilder builder;
        for (std::string s : strings) {
            builder.PutString(s);
        }
        auto result = BinaryBuffer(builder);
        builder.Detach();
        return result;
    }

    std::vector<std::string> threeStrings;
    std::vector<std::string> oneString;
    BinaryBuffer             emptyBuffer;
    BinaryBuffer             threeStringsBuffer;
    BinaryBuffer             oneStringBuffer;
    BufferChain              chain;
};

TEST_F(TestBlockIterator, EmptyHasNotNext) {
    BlockIterator<std::string> it(chain);
    ASSERT_FALSE(it.HasNext());
}

TEST_F(TestBlockIterator, EmptyIsNotClosed) {
    BlockIterator<std::string> it(chain);
    ASSERT_FALSE(it.IsClosed());
}

TEST_F(TestBlockIterator, ClosedReturnsIsClosed) {
    chain.Append(oneStringBuffer);
    chain.Close();
    BlockIterator<std::string> it(chain);
    ASSERT_TRUE(it.IsClosed());
}

TEST_F(TestBlockIterator, IterateOverStringsInSingleBuffer) {
    chain.Append(oneStringBuffer);
    chain.Append(threeStringsBuffer);
    BlockIterator<std::string> it(chain);
    ASSERT_EQ(".", it.Next());
    ASSERT_EQ("foo", it.Next());
}

TEST_F(TestBlockIterator, IterateOverStringsInTwoBuffers) {
    chain.Append(threeStringsBuffer);
    BlockIterator<std::string> it(chain);
    ASSERT_EQ("foo", it.Next());
    ASSERT_EQ("bar", it.Next());
}

TEST_F(TestBlockIterator, HasNextReturnsFalseAtTheEnd) {
    chain.Append(threeStringsBuffer);
    BlockIterator<std::string> it(chain);
    (void)it.Next();
    (void)it.Next();
    (void)it.Next();
    ASSERT_FALSE(it.HasNext());
}

TEST_F(TestBlockIterator, IsClosedReturnsFalseAtTheEnd) {
    chain.Append(threeStringsBuffer);
    BlockIterator<std::string> it(chain);
    (void)it.Next();
    (void)it.Next();
    (void)it.Next();
    ASSERT_FALSE(it.IsClosed());
}

TEST_F(TestBlockIterator, HasNextReturnsTrueInTheMiddle) {
    chain.Append(threeStringsBuffer);
    BlockIterator<std::string> it(chain);
    (void)it.Next();
    ASSERT_TRUE(it.HasNext());
}

TEST_F(TestBlockIterator, HasNextReturnsTrueBetweenBuffers) {
    chain.Append(threeStringsBuffer);
    chain.Append(oneStringBuffer);
    BlockIterator<std::string> it(chain);
    (void)it.Next();
    (void)it.Next();
    (void)it.Next();
    ASSERT_TRUE(it.HasNext());
}
/******************************************************************************/
