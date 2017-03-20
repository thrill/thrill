/*******************************************************************************
 * tests/api/reduce_node_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/all_gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>

#include <thrill/common/logger.hpp>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT
using core::ReduceTableImpl;

struct MyStruct {
    size_t x;
};

struct MyStructHash {
    size_t operator () (const MyStruct& a) const {
        return hasher_(a.x);
    }
    std::hash<size_t> hasher_;
};

struct MyStructEqual {
    size_t operator () (const MyStruct& a, const MyStruct& b) const {
        return a.x == b.x;
    }
};

template <ReduceTableImpl table_impl>
class TestReduceModulo2CorrectResults
{
public:
    void operator () (Context& ctx) {
        auto integers = Generate(
            ctx, 16,
            [](const size_t& index) {
                return index + 1;
            });

        auto modulo_two = [](size_t in) {
                              return MyStruct { in % 4 };
                          };

        auto add_function = [](const size_t& in1, const size_t& in2) {
                                return in1 + in2;
                            };

        auto reduced = integers.ReduceByKey(
            VolatileKeyTag, modulo_two, add_function,
            core::DefaultReduceConfigSelect<table_impl>(),
            MyStructHash(), MyStructEqual());

        std::vector<size_t> out_vec = reduced.AllGather();
        ASSERT_EQ(4u, out_vec.size());

        std::sort(out_vec.begin(), out_vec.end());

        size_t i = 1;
        for (const size_t& element : out_vec) {
            ASSERT_EQ(element, 24 + (4 * i++));
        }
    }
};

TEST(ReduceNode, ReduceModulo2CorrectResults) {
    api::RunLocalTests(
        TestReduceModulo2CorrectResults<ReduceTableImpl::PROBING>());
    api::RunLocalTests(
        TestReduceModulo2CorrectResults<ReduceTableImpl::BUCKET>());
    api::RunLocalTests(
        TestReduceModulo2CorrectResults<ReduceTableImpl::OLD_PROBING>());
}

//! Test sums of integers 0..n-1 for n=100 in 1000 buckets in the reduce table
template <ReduceTableImpl table_impl>
class TestReduceModuloPairsCorrectResults
{
public:
    void operator () (Context& ctx) {
        static constexpr size_t test_size = 1000000u;
        static constexpr size_t mod_size = 1000u;
        static constexpr size_t div_size = test_size / mod_size;

        using IntPair = std::pair<size_t, size_t>;

        auto integers = Generate(
            ctx, test_size,
            [](const size_t& index) {
                return IntPair(index % mod_size, index / mod_size);
            });

        auto add_function = [](const size_t& in1, const size_t& in2) {
                                return in1 + in2;
                            };

        auto reduced = integers.ReducePair(
            add_function,
            core::DefaultReduceConfigSelect<table_impl>());

        std::vector<IntPair> out_vec = reduced.AllGather();

        std::sort(out_vec.begin(), out_vec.end(),
                  [](const IntPair& p1, const IntPair& p2) {
                      return p1.first < p2.first;
                  });

        ASSERT_EQ(mod_size, out_vec.size());
        for (const auto& element : out_vec) {
            ASSERT_EQ(element.second, (div_size * (div_size - 1)) / 2u);
        }
    }
};

TEST(ReduceNode, ReduceModuloPairsCorrectResults) {
    api::RunLocalTests(
        TestReduceModuloPairsCorrectResults<ReduceTableImpl::PROBING>());
    api::RunLocalTests(
        TestReduceModuloPairsCorrectResults<ReduceTableImpl::BUCKET>());
    api::RunLocalTests(
        TestReduceModuloPairsCorrectResults<ReduceTableImpl::OLD_PROBING>());
}

template <ReduceTableImpl table_impl>
class TestReduceToIndexCorrectResults
{
public:
    void operator () (Context& ctx) {

        auto integers = Generate(
            ctx, 16,
            [](const size_t& index) {
                return index + 1;
            });

        auto key = [](size_t in) {
                       return in / 2;
                   };

        auto add_function = [](const size_t& in1, const size_t& in2) {
                                return in1 + in2;
                            };

        size_t result_size = 9;

        auto reduced = integers.ReduceToIndex(
            VolatileKeyTag, key, add_function, result_size,
            /* neutral_element */ size_t(),
            core::DefaultReduceConfigSelect<table_impl>());

        std::vector<size_t> out_vec = reduced.AllGather();
        ASSERT_EQ(9u, out_vec.size());

        size_t i = 0;
        for (size_t element : out_vec) {
            switch (i++) {
            case 0:
                ASSERT_EQ(1u, element);
                break;
            case 1:
                ASSERT_EQ(5u, element);
                break;
            case 2:
                ASSERT_EQ(9u, element);
                break;
            case 3:
                ASSERT_EQ(13u, element);
                break;
            case 4:
                ASSERT_EQ(17u, element);
                break;
            case 5:
                ASSERT_EQ(21u, element);
                break;
            case 6:
                ASSERT_EQ(25u, element);
                break;
            case 7:
                ASSERT_EQ(29u, element);
                break;
            case 8:
                ASSERT_EQ(16u, element);
                break;
            default:
                ASSERT_EQ(42, 420);
            }
        }
    }
};

TEST(ReduceNode, ReduceToIndexCorrectResults) {
    api::RunLocalTests(
        TestReduceToIndexCorrectResults<ReduceTableImpl::PROBING>());
    api::RunLocalTests(
        TestReduceToIndexCorrectResults<ReduceTableImpl::BUCKET>());
    api::RunLocalTests(
        TestReduceToIndexCorrectResults<ReduceTableImpl::OLD_PROBING>());
}

TEST(ReduceToIndexNode, OutputSizeCheck) {
    auto start_func =
        [](Context& context) {
            size_t node_count = 20000;
            size_t result = Generate(context, 10000, [node_count](const size_t index) { return index % node_count; })
                .Filter([](const size_t node) { return node % 1000 < 250; })
                .ReduceToIndex(
                    [](const size_t& node) -> size_t { return node; },
                    [](const size_t node, const size_t) {
                        return node;
                    }, node_count)
                .Size();
            ASSERT_EQ(node_count, result);
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/

