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
#include <typeinfo>


#include "gtest/gtest.h"

using namespace c7a::data;

static const bool debug = false;

TEST(Serializer, StringSerializeDeserialize) {
    std::string foo = "foo";
    auto fooserial = Deserialize<std::string>(Serialize<std::string>(foo));
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, IntSerializeDeserialize) {
    int foo = -123;
    auto fooserial = Deserialize<int>(Serialize<int>(foo));
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, LongSerializeDeserialize) {
    long foo = -123;
    auto fooserial = Deserialize<long>(Serialize<long>(foo));
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, LongLongSerializeDeserialize) {
    long long foo = -123;
    auto fooserial = Deserialize<long long>(Serialize<long long>(foo));
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, UnsignedSerializeDeserialize) {
    unsigned int foo = 2154910440;
    auto fooserial = Deserialize<unsigned int>(Serialize<unsigned int>(foo));
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
    ASSERT_EQ(sizeof(foo), sizeof(fooserial));
}

TEST(Serializer, UnsignedLongSerializeDeserialize) {
    unsigned long foo = 123;
    auto fooserial = Deserialize<unsigned long>(Serialize<unsigned long>(foo));
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, UnsignedLongLongSerializeDeserialize) {
    unsigned long long foo = 123;
    auto fooserial = Deserialize<unsigned long long>(Serialize<unsigned long long>(foo));
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, FloatSerializeDeserialize) {
    float foo = 123.123;
    auto fooserial = Deserialize<float>(Serialize<float>(foo));
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, DoubleSerializeDeserialize) {
    double foo = 123.123;
    auto fooserial = Deserialize<double>(Serialize<double>(foo));
    ASSERT_DOUBLE_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, LongDoubleSerializeDeserialize) {
    long double foo = 123.123;
    auto fooserial = Deserialize<long double>(Serialize<long double>(foo));
    ASSERT_DOUBLE_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, SizeTSerializeDeserialize) {
    size_t foo = 123;
    auto fooserial = Deserialize<size_t>(Serialize<size_t>(foo));
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
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

TEST(Serializer, Tuple_SerializeDeserialize_Test) {
    std::tuple<int, std::string, int> foo = std::make_tuple (3, "foo", 5);
    auto res = Serialize<std::tuple<int, std::string, int>>(foo);
    LOG1 << res;
}

/******************************************************************************/
