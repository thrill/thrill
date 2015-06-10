/*******************************************************************************
 * tests/data/serializer_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/logger.hpp>
#include <c7a/data/serializer.hpp>

#include <utility>
#include <string>


#include "gtest/gtest.h"

using namespace c7a::data;

static const bool debug = true;

TEST(Serializer, StringSerializeDeserialize) {
    ASSERT_EQ("foo", Deserialize<std::string>(Serialize<std::string>("foo")));
}

TEST(Serializer, IntSerializeDeserialize) {
    ASSERT_EQ(-123, Deserialize<int>(Serialize<int>(-123)));
}

TEST(Serializer, LongSerializeDeserialize) {
    long x = 123;
    ASSERT_EQ(x, Deserialize<long>(Serialize<long>(x)));
}

TEST(Serializer, LongLongSerializeDeserialize) {
    long long x = 123;
    ASSERT_EQ(x, Deserialize<long long>(Serialize<long long>(x)));
}

TEST(Serializer, UnsignedSerializeDeserialize) {
    unsigned x = 123;
    ASSERT_EQ(x, Deserialize<unsigned>(Serialize<unsigned>(x)));
}

TEST(Serializer, UnsignedLongSerializeDeserialize) {
    unsigned long x = 123;
    ASSERT_EQ(x, Deserialize<unsigned long>(Serialize<unsigned long>(x)));
}

TEST(Serializer, UnsignedLongLongSerializeDeserialize) {
    unsigned long long x = 123;
    ASSERT_EQ(x, Deserialize<unsigned long long>(Serialize<unsigned long long>(x)));
}

TEST(Serializer, FloatSerializeDeserialize) {
    float x = 123;
    ASSERT_EQ(x, Deserialize<float>(Serialize<float>(x)));
}

TEST(Serializer, DoubleSerializeDeserialize) {
    double x = 123;
    ASSERT_EQ(x, Deserialize<double>(Serialize<double>(x)));
}

TEST(Serializer, LongDoubleSerializeDeserialize) {
    long double x = 123;
    ASSERT_EQ(x, Deserialize<long double>(Serialize<long double>(x)));
}

TEST(Serializer, SizeTSerializeDeserialize) {
    size_t x = 123;
    ASSERT_EQ(x, Deserialize<size_t>(Serialize<size_t>(x)));
}

TEST(Serializer, StringIntPairSerializeDeserialize) {
    auto t = std::make_pair("foo", 123);
    auto serialized = Serialize<std::pair<std::string, int> >(t);
    auto result = Deserialize<std::pair<std::string, int> >(serialized);
    ASSERT_EQ(std::get<0>(t), std::get<0>(result));
    ASSERT_EQ(std::get<1>(t), std::get<1>(result));
}

TEST(Serializer, StringString_Pair_SerializeDeserialize_Test) {
    auto t1 = "first";
    auto t2 = "second";
    auto t = std::make_pair(t1, t2);

    auto serialized = Serialize<std::pair<std::string, std::string> >(t);
    auto result = Deserialize<std::pair<std::string, std::string> >(serialized);

    ASSERT_EQ(std::get<0>(t), std::get<0>(result));
    ASSERT_EQ(std::get<1>(t), std::get<1>(result));
}

TEST(Serializer, IntInt_Pair_SerializeDeserialize_Test) {
    auto t1 = 3;
    auto t2 = 4;
    auto t = std::make_pair(t1, t2);

    auto serialized = Serialize<std::pair<int, int> >(t);
    auto result = Deserialize<std::pair<int, int> >(serialized);

    ASSERT_EQ(std::get<0>(t), std::get<0>(result));
    ASSERT_EQ(std::get<1>(t), std::get<1>(result));
}


/******************************************************************************/
