/*******************************************************************************
 * tests/api/zip_node_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/allgather.hpp>
#include <c7a/api/bootstrap.hpp>
#include <c7a/api/generate.hpp>
#include <c7a/api/lop_node.hpp>
#include <c7a/api/zip.hpp>
#include <c7a/c7a.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>

using namespace c7a;

using c7a::api::Context;
using c7a::api::DIARef;

struct MyStruct {
    int a, b;
    MyStruct(int _a, int _b) : a(_a), b(_b) { }
};

namespace c7a {
namespace data {

template <typename Archive>
struct Serializer<Archive, MyStruct>
{
    static void serialize(const MyStruct& x, Archive& ar) {
        Serializer<Archive, int>::serialize(x.a, ar);
        Serializer<Archive, int>::serialize(x.b, ar);
    }
    static MyStruct deserialize(Archive& ar) {
        int a = Serializer<Archive, int>::deserialize(ar);
        int b = Serializer<Archive, int>::deserialize(ar);
        return MyStruct(a, b);
    }
    static const bool fixed_size = (Serializer<Archive, int>::fixed_size &&
                                    Serializer<Archive, int>::fixed_size);
};

} // namespace data
} // namespace c7a

static const size_t test_size = 1000;

TEST(ZipNode, TwoBalancedIntegerArrays) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // numbers 0..999 (evenly distributed to workers)
            auto zip_input1 = Generate(
                ctx,
                [](size_t index) { return index; },
                test_size);

            // numbers 1000..1999
            auto zip_input2 = zip_input1.Map(
                [](size_t i) { return static_cast<short>(test_size + i); });

            // zip
            auto zip_result = zip_input1.Zip(
                zip_input2, [](size_t a, short b) -> long { return a + b; });

            // check result
            std::vector<long> res = zip_result.AllGather();

            ASSERT_EQ(test_size, res.size());

            for (size_t i = 0; i != res.size(); ++i) {
                ASSERT_EQ(static_cast<long>(i + i + test_size), res[i]);
            }
        };

    c7a::api::ExecuteLocalTests(start_func);
}

TEST(ZipNode, TwoDisbalancedIntegerArrays) {

    // first DIA is heavily balanced to the first workers, second DIA is
    // balanced to the last workers.
    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // numbers 0..999 (evenly distributed to workers)
            auto input1 = Generate(
                ctx,
                [](size_t index) { return index; },
                test_size);

            // numbers 1000..1999
            auto input2 = input1.Map(
                [](size_t i) { return static_cast<short>(test_size + i); });

            // numbers 0..99 (concentrated on first workers)
            auto zip_input1 = input1.Filter(
                [](size_t i) { return i < test_size / 10; });

            // numbers 1900..1999 (concentrated on last workers)
            auto zip_input2 = input2.Filter(
                [](size_t i) { return i >= 2 * test_size - test_size / 10; });

            // zip
            auto zip_result = zip_input1.Zip(
                zip_input2, [](size_t a, short b) -> MyStruct { return MyStruct(a, b); });

            // check result
            std::vector<MyStruct> res = zip_result.AllGather();

            ASSERT_EQ(test_size / 10, res.size());

            for (size_t i = 0; i != res.size(); ++i) {
                //sLOG1 << i << res[i].a << res[i].b;
                ASSERT_EQ(static_cast<long>(i), res[i].a);
                ASSERT_EQ(static_cast<long>(2 * test_size - test_size / 10 + i), res[i].b);
            }

            // TODO(sl): make this work!
            // check size of zip (recalculates ZipNode)
            ASSERT_EQ(100u, zip_result.Size());
        };

    c7a::api::ExecuteLocalTests(start_func);
}

TEST(ZipNode, TwoIntegerArraysWhereOneIsEmpty) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // numbers 0..999 (evenly distributed to workers)
            auto input1 = Generate(
                ctx,
                [](size_t index) { return index; },
                test_size);

            // numbers 0..999 (evenly distributed to workers)
            auto input2 = Generate(
                ctx,
                [](size_t index) { return index; },
                0);

            // zip
            auto zip_result = input1.Zip(
                input2, [](size_t a, short b) -> long { return a + b; });

            // check result
            std::vector<long> res = zip_result.AllGather();
            ASSERT_EQ(0u, res.size());
        };

    c7a::api::ExecuteLocalTests(start_func);
}

/******************************************************************************/
