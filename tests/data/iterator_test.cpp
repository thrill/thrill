/*******************************************************************************
 * tests/data/iterator_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/data/iterator.hpp>
#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace c7a::data;
using namespace c7a::net;

struct TestIterator : public::testing::Test {
    TestIterator() :
        threeStrings({ "foo", "bar", "blub" }),
        oneString({ "." }),
        fourStrings({ "a", "bc", "def", "ghij" }),
        emptyBuffer(nullptr, 0),
        threeStringsBuffer(StringsToBufferBuilder(threeStrings)),
        oneStringBuffer(StringsToBufferBuilder(oneString)),
        fourStringsBuffer(StringsToBufferBuilder(fourStrings)) { }

    BinaryBufferBuilder StringsToBufferBuilder(std::vector<std::string> strings) const {
        BinaryBufferBuilder builder;
        for (std::string s : strings) {
            builder.PutString(s);
        }
        return builder;
    }

    std::vector<std::string> DataToStringVector(void* data, size_t len) {
        std::vector<std::string> result;
        BinaryBufferReader reader(BinaryBuffer(data, len));
        while (!reader.empty())
            result.push_back(reader.GetString());
        return result;
    }

    std::vector<std::string> threeStrings;
    std::vector<std::string> oneString;
    std::vector<std::string> fourStrings;
    BinaryBufferBuilder      emptyBuffer;
    BinaryBufferBuilder      threeStringsBuffer;
    BinaryBufferBuilder      oneStringBuffer;
    BinaryBufferBuilder      fourStringsBuffer;
    BufferChain              chain;
};

TEST_F(TestIterator, EmptyHasNotNext) {
    Iterator<std::string> it(chain);
    ASSERT_FALSE(it.HasNext());
}

TEST_F(TestIterator, EmptyIsNotClosed) {
    Iterator<std::string> it(chain);
    ASSERT_FALSE(it.IsFinished());
}

TEST_F(TestIterator, ClosedIsNotFinished) {
    chain.Append(oneStringBuffer);
    chain.Close();
    Iterator<std::string> it(chain);
    ASSERT_FALSE(it.IsFinished());
}

TEST_F(TestIterator, ClosedIsFinishedWhenAtEnd) {
    chain.Append(oneStringBuffer);
    chain.Close();
    Iterator<std::string> it(chain);
    it.Next();
    ASSERT_TRUE(it.IsFinished());
}

TEST_F(TestIterator, IterateOverStringsInSingleBuffer) {
    chain.Append(oneStringBuffer);
    chain.Append(threeStringsBuffer);
    Iterator<std::string> it(chain);
    ASSERT_EQ(".", it.Next());
    ASSERT_EQ("foo", it.Next());
}

TEST_F(TestIterator, IterateOverStringsInTwoBuffers) {
    chain.Append(threeStringsBuffer);
    Iterator<std::string> it(chain);
    ASSERT_EQ("foo", it.Next());
    ASSERT_EQ("bar", it.Next());
}

TEST_F(TestIterator, HasNextReturnsFalseAtTheEnd) {
    chain.Append(threeStringsBuffer);
    Iterator<std::string> it(chain);
    (void)it.Next();
    (void)it.Next();
    (void)it.Next();
    ASSERT_FALSE(it.HasNext());
}

TEST_F(TestIterator, IsFinishedReturnsFalseAtTheEnd) {
    chain.Append(threeStringsBuffer);
    Iterator<std::string> it(chain);
    (void)it.Next();
    (void)it.Next();
    (void)it.Next();
    ASSERT_FALSE(it.IsFinished());
}

TEST_F(TestIterator, HasNextReturnsTrueInTheMiddle) {
    chain.Append(threeStringsBuffer);
    Iterator<std::string> it(chain);
    (void)it.Next();
    ASSERT_TRUE(it.HasNext());
}

TEST_F(TestIterator, HasNextReturnsTrueBetweenBuffers) {
    chain.Append(threeStringsBuffer);
    chain.Append(oneStringBuffer);
    Iterator<std::string> it(chain);
    (void)it.Next();
    (void)it.Next();
    (void)it.Next();
    ASSERT_TRUE(it.HasNext());
}

TEST_F(TestIterator, SeekOnEmptyReturnsZeros) {
    chain.Close();
    Iterator<std::string> it(chain);
    void* data;
    size_t len;
    ASSERT_EQ(it.Seek(42, &data, &len), 0u);
    ASSERT_EQ(data, nullptr);
    ASSERT_EQ(len, 0u);
}

TEST_F(TestIterator, SeeksOnlyAvailableElements) {
    chain.Append(threeStringsBuffer);
    chain.Close();
    Iterator<std::string> it(chain);
    void* data;
    size_t len;
    ASSERT_EQ(it.Seek(42, &data, &len), 3u);
}

TEST_F(TestIterator, SeeksFromBeginReturnsCorrectData) {
    chain.Append(threeStringsBuffer);
    chain.Close();
    Iterator<std::string> it(chain);
    void* data;
    size_t len;
    it.Seek(2, &data, &len);
    auto elements = DataToStringVector(data, len);
    ASSERT_EQ(2u, elements.size());
    ASSERT_EQ(threeStrings[0], elements[0]);
    ASSERT_EQ(threeStrings[1], elements[1]);
}

TEST_F(TestIterator, SeeksFromMiddleReturnsCorrectData) {
    chain.Append(threeStringsBuffer);
    chain.Close();
    Iterator<std::string> it(chain);
    void* data;
    size_t len;
    it.Seek(1, &data, &len);
    it.Seek(2, &data, &len);
    auto elements = DataToStringVector(data, len);
    ASSERT_EQ(2u, elements.size());
    ASSERT_EQ(threeStrings[1], elements[0]);
    ASSERT_EQ(threeStrings[2], elements[1]);
}

TEST_F(TestIterator, MultipleSeeksOverMultibleBuffers) {
    chain.Append(threeStringsBuffer);
    chain.Append(oneStringBuffer);
    chain.Append(fourStringsBuffer);
    chain.Close();
    Iterator<std::string> it(chain);
    void* data;
    size_t len;
    size_t length1 = threeStrings[0].size() + threeStrings[1].size() + threeStrings[2].size() + 3 * sizeof(uint8_t);
    size_t length2 = oneString[0].size() + sizeof(uint8_t);
    size_t length3 = fourStrings[0].size() + fourStrings[1].size() + 2 * sizeof(uint8_t);
    size_t length4 = fourStrings[2].size() + sizeof(uint8_t);

    ASSERT_EQ(3u, it.Seek(7, &data, &len));
    ASSERT_EQ(len, length1);
    ASSERT_EQ(1u, it.Seek(4, &data, &len));
    ASSERT_EQ(len, length2);
    ASSERT_EQ(2u, it.Seek(2, &data, &len));
    ASSERT_EQ(len, length3);
    ASSERT_EQ(1u, it.Seek(1, &data, &len));
    ASSERT_EQ(len, length4);
}

TEST_F(TestIterator, NextAfterSeek) {
    chain.Append(threeStringsBuffer);
    chain.Close();
    Iterator<std::string> it(chain);
    void* data;
    size_t len;
    it.Seek(2, &data, &len);
    ASSERT_TRUE(it.HasNext());
    ASSERT_EQ(threeStrings[2], it.Next());
    ASSERT_FALSE(it.HasNext());
}
/******************************************************************************/
