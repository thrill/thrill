/*******************************************************************************
 * tests/data/serialization_test.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>
#include <thrill/data/block_queue.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/serialization.hpp>

#include <string>
#include <tuple>
#include <typeinfo>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

static const bool debug = false;

struct Serialization : public::testing::Test {
    data::BlockPool block_pool_ { nullptr };
};

TEST_F(Serialization, string) {
    data::File f(block_pool_);
    std::string foo = "foo";
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetConstReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST_F(Serialization, int) {
    int foo = -123;
    data::File f(block_pool_);
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetConstReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST_F(Serialization, pair_string_int) {
    auto foo = std::make_pair(std::string("foo"), 123);
    data::File f(block_pool_);
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetConstReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
}

TEST_F(Serialization, pair_int_int) {
    int t1 = 3;
    int t2 = 4;
    std::pair<int, int> foo = std::make_pair(t1, t2);
    data::File f(block_pool_);
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetConstReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
}

struct MyPodStruct
{
    int    i1;
    double d2;
};

TEST_F(Serialization, pod_struct) {
    MyPodStruct foo = { 6 * 9, 42 };
    data::File f(block_pool_);
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetConstReader();
    auto fooserial = r.Next<MyPodStruct>();
    ASSERT_EQ(foo.i1, fooserial.i1);
    ASSERT_FLOAT_EQ(foo.d2, fooserial.d2);
    static_assert(
        data::Serialization<data::DynBlockWriter, MyPodStruct>::is_fixed_size,
        "Serialization::is_fixed_size is wrong");
    static_assert(
        data::Serialization<data::DynBlockWriter, MyPodStruct>::fixed_size
        == sizeof(MyPodStruct),
        "Serialization::fixed_size is wrong");
}

TEST_F(Serialization, tuple) {
    auto foo = std::make_tuple(3, std::string("foo"), 5.5);
    data::File f(block_pool_);
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetConstReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
    ASSERT_EQ(std::get<2>(foo), std::get<2>(fooserial));
    static_assert(
        !data::Serialization<data::DynBlockWriter, decltype(foo)>::is_fixed_size,
        "Serialization::is_fixed_size is wrong");
}

TEST_F(Serialization, tuple_w_pair) {
    auto p = std::make_pair(-4.673, std::string("string"));
    auto foo = std::make_tuple(3, std::string("foo"), 5.5, p);
    data::File f(block_pool_);
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    ASSERT_EQ(1u, f.num_items());
    auto r = f.GetConstReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
    ASSERT_EQ(std::get<2>(foo), std::get<2>(fooserial));
    ASSERT_FLOAT_EQ(std::get<3>(foo).first, std::get<3>(fooserial).first);
    ASSERT_EQ(std::get<3>(foo).second, std::get<3>(fooserial).second);
}

TEST_F(Serialization, tuple_check_fixed_size) {
    data::File f(block_pool_);
    auto n = std::make_tuple(1, 2, 3, std::string("blaaaa"));
    auto y = std::make_tuple(1, 2, 3, 42.0);
    auto no = data::Serialization<data::DynBlockWriter, decltype(n)>::is_fixed_size;
    auto yes = data::Serialization<data::DynBlockWriter, decltype(y)>::is_fixed_size;

    ASSERT_EQ(no, false);
    ASSERT_EQ(yes, true);
}

TEST_F(Serialization, StringVector) {
    std::vector<std::string> vec1 = {
        "what", "a", "wonderful", "world", "this", "could", "be"
    };
    data::File f(block_pool_);
    {
        auto w = f.GetWriter();
        w(vec1);
        w(static_cast<int>(42));
    }
    ASSERT_EQ(2u, f.num_items());
    auto r = f.GetConstReader();
    auto vec2 = r.Next<std::vector<std::string> >();
    ASSERT_EQ(7u, vec1.size());
    ASSERT_EQ(vec1, vec2);

    auto check42 = r.Next<int>();
    ASSERT_EQ(42, check42);
}

TEST_F(Serialization, StringArray) {
    std::array<std::string, 7> vec1 = {
        { "what", "a", "wonderful", "world", "this", "could", "be" }
    };
    data::File f(block_pool_);
    {
        auto w = f.GetWriter();
        w(vec1);
        w(static_cast<int>(42));
    }
    ASSERT_EQ(2u, f.num_items());
    auto r = f.GetConstReader();
    auto vec2 = r.Next<std::array<std::string, 7> >();
    ASSERT_EQ(7u, vec1.size());
    ASSERT_EQ(vec1, vec2);

    auto check42 = r.Next<int>();
    ASSERT_EQ(42, check42);
}

/******************************************************************************/
