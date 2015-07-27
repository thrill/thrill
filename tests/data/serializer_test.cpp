/*******************************************************************************
 * tests/data/serializer_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/common/logger.hpp>
#include <c7a/data/block_queue.hpp>
#include <c7a/data/file.hpp>
#include <c7a/data/serializer.hpp>
#include <gtest/gtest.h>

#include <string>
#include <tuple>
#include <typeinfo>
#include <utility>

using namespace c7a::data;

static const bool debug = false;

TEST(Serializer, string) {
    c7a::data::File f;
    std::string foo = "foo";
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, int) {
    int foo = -123;
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, pair_string_int) {
    auto foo = std::make_pair("foo", 123);
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
}

TEST(Serializer, pair_int_int) {
    auto t1 = 3;
    auto t2 = 4;
    auto foo = std::make_pair(t1, t2);
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
}

TEST(Serializer, tuple) {
    auto foo = std::make_tuple(3, "foo", 5.5);
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
    ASSERT_EQ(std::get<2>(foo), std::get<2>(fooserial));
}

TEST(Serializer, tuple_w_pair) {
    auto p = std::make_pair(-4.673, "string");
    auto foo = std::make_tuple(3, "foo", 5.5, p);
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    ASSERT_EQ(1u, f.NumItems());
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
    ASSERT_EQ(std::get<2>(foo), std::get<2>(fooserial));
    ASSERT_FLOAT_EQ(std::get<3>(foo).first, std::get<3>(fooserial).first);
    ASSERT_EQ(std::get<3>(foo).second, std::get<3>(fooserial).second);
}

// TEST(Serializer, ProtoBuf_Test) {
//     ProtobufObject t(1, 2);
//     auto serialized = Serialize<ProtobufObject>(t);
//     auto result = Deserialize<ProtobufObject>(serialized);
//     ASSERT_EQ(t.bla_, result.bla_);
//     ASSERT_EQ(t.blu_, result.blu_);
// }

TEST(Serializer, tuple_check_fixed_size) {
    c7a::data::File f;
    auto n = std::make_tuple(1, 2, 3, std::string("blaaaa"));
    auto y = std::make_tuple(1, 2, 3, "4");
    auto w = f.GetWriter();
    auto no = Serializer<decltype(w), decltype(n)>::fixed_size;
    auto yes = Serializer<decltype(w), decltype(y)>::fixed_size;

    ASSERT_EQ(no, false);
    ASSERT_EQ(yes, true);
}

/******************************************************************************/
