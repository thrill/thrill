/*******************************************************************************
 * tests/api/merge_node_test.cpp
 *
 * Part of Project thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gemail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/api/allgather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/merge.hpp>
#include <thrill/common/string.hpp>
#include <gtest/gtest.h>
#include <thrill/data/block_queue.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <vector>

using namespace thrill;

using thrill::api::Context;
using thrill::api::DIARef;
using thrill::api::MergeNodeHelper;

struct MyStruct {
    int a, b;
};

struct MergeHelpers : public::testing::Test {
    data::BlockPool block_pool_ { nullptr };
};

void CreateTrivialFiles(std::vector<data::File> &files, size_t size, size_t count, data::BlockPool &block_pool_) {
    files.reserve(count);

    for(size_t i = 0; i < count; i++) {
        files.emplace_back(block_pool_);
        data::File::Writer fw = files[i].GetWriter(53);

        for (size_t j = 0; j < size; j++) {
            fw(j);
        }

        fw.Close();
    }
}

void CreateRandomSizeFiles(std::vector<data::File> &files, size_t maxSize, size_t count, data::BlockPool &block_pool_) {
    files.reserve(count);

    std::mt19937 gen(0);

    for(size_t i = 0; i < count; i++) {
        files.emplace_back(block_pool_);
        data::File::Writer fw = files[i].GetWriter(53);

        size_t size = gen() % maxSize;
        for (size_t j = 0; j < size; j++) {
            fw(j);
        }

        fw.Close();
    }
}

TEST_F(MergeHelpers, MultiIndexOf) {
    const size_t size = 500;
    const size_t count = 10;

    std::vector<data::File> files;
    CreateTrivialFiles(files, size, count, block_pool_);

    for (size_t i = 0; i < size; i++) {
        size_t val = i;

        size_t idx = MergeNodeHelper::IndexOf(val, 0, files, std::less<size_t>());

        ASSERT_EQ(val, idx / count);
        
        size_t val2 = MergeNodeHelper::GetAt<size_t, std::less<size_t>>(idx, files, std::less<size_t>());
        ASSERT_EQ(val, val2);
    }
}

TEST_F(MergeHelpers, MultiGetAtIndex) {
    const size_t size = 500;
    const size_t count = 10;

    std::vector<data::File> files;
    CreateTrivialFiles(files, size, count, block_pool_);

    for (size_t i = 0; i < (size * count) / 17; i++) {
        size_t idx = (i * 17);

        size_t val = MergeNodeHelper::GetAt<size_t, std::less<size_t>>(idx, files, std::less<size_t>());

        ASSERT_EQ(idx / count, val);
    }
}
template <typename stackA, typename stackB>
void DoMergeAndCheckResult(api::DIARef<size_t, stackA> merge_input1, api::DIARef<size_t, stackB> merge_input2, size_t expected_size, int num_workers) {
        // merge
        auto merge_result = merge_input1.Merge(
            merge_input2, std::less<size_t>());

        // check if order was kept while merging. 
        int count = 0;
        auto res = merge_result.Map([&count] (size_t in) { 
                count++; 
                return in; 
        }).AllGather();

        ASSERT_EQ(expected_size, res.size());

        for (size_t i = 0; i != res.size() - 1; ++i) {
            ASSERT_TRUE(res[i] < res[i + 1]);
        }

        // check if balancing condition was met
        ASSERT_TRUE(abs((int)res.size() / num_workers - count) < 10);
}

TEST(MergeNode, TwoBalancedIntegerArrays) {

    const size_t test_size = 50;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // even numbers in 0..98 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx,
                [](size_t index) { return index * 2; },
                test_size);

            // odd numbers in 1..99
            auto merge_input2 = merge_input1.Map(
                [](size_t i) { return i + 1; } );

            DoMergeAndCheckResult(merge_input1, merge_input2, test_size * 2, ctx.num_workers());
        };

    thrill::api::RunLocalTests(start_func);
}

TEST(MergeNode, TwoImbalancedIntegerArrays) {

    const size_t test_size = 50;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // numbers in 0..50 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx,
                [](size_t index) { return index ; },
                test_size);

            // numbers in 100..150
            auto merge_input2 = merge_input1.Map(
                [](size_t i) { return i + 100; } );

            DoMergeAndCheckResult(merge_input1, merge_input2, test_size * 2, ctx.num_workers());
        };

    thrill::api::RunLocalTests(start_func);
}

TEST(MergeNode, TwoIntegerArraysOfDifferentSize) {

    const size_t test_size = 50;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // numbers in 0..50 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx,
                [](size_t index) { return index ; },
                test_size);

            // numbers in 10..110
            auto merge_input2 = Generate(
                ctx,
                [](size_t index) { return index + 10; },
                test_size * 2);

            DoMergeAndCheckResult(merge_input1, merge_input2, test_size * 3, ctx.num_workers());
        };

    thrill::api::RunLocalTests(start_func);
}
