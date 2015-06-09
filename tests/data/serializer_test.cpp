/*******************************************************************************
 * tests/data/serializer_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
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

TEST(Serializer, StringIntPairSerializeDeserialize) {
    auto t = std::make_pair("foo", 123);
    auto serialized = Serialize<std::pair<std::string, int> >(t);
    auto result = Deserialize<std::pair<std::string, int> >(serialized);
    ASSERT_EQ(std::get<0>(t), std::get<0>(result));
    ASSERT_EQ(std::get<1>(t), std::get<1>(result));
}

TEST(Serializer, PairSerializeDeserialize) {
    auto t1 = "first";
    auto t2 = "second";
    auto t = std::make_pair(t1, t2);

    auto serialized = Serialize<std::pair<std::string, std::string> >(t);
    sLOG << serialized;
    auto result = Deserialize<std::pair<std::string, std::string> >(serialized);

    // ASSERT_EQ(std::get<0>(t), std::get<0>(result));
    // ASSERT_EQ(std::get<1>(t), std::get<1>(result));
    sLOG << "START" << result.second << "END";
}

/******************************************************************************/
