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

struct File : public::testing::Test {
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

TEST_F(File, MultiIndexOf) {
    const size_t size = 500;
    const size_t count = 10;

    std::minstd_rand0 rng(0);
    std::vector<data::File> files;
    CreateTrivialFiles(files, size, count, block_pool_);

    for (size_t i = 0; i < 10; i++) {
        size_t val = rng() % size;

        size_t idx = MergeNodeHelper::IndexOf(val, 0, files, [](size_t a, size_t b) -> int { return b - a; });

        ASSERT_EQ(val, idx / count);
    }
}

TEST_F(File, MultiGetAtIndex) {
    const size_t size = 500;
    const size_t count = 10;

    std::minstd_rand0 rng(0);
    std::vector<data::File> files;
    CreateTrivialFiles(files, size, count, block_pool_);

    for (size_t i = 0; i < 10; i++) {
        size_t idx = rng() % size * count;

        size_t val = MergeNodeHelper::GetAt<size_t, std::function<size_t(size_t, size_t)>>(idx, files, [](size_t a, size_t b) -> size_t { return b - a; });

        ASSERT_EQ(idx / count, val);
    }
}
/*
TEST(MergeNode, TwoBalancedIntegerArrays) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            // even numbers in 0..998 (evenly distributed to workers)
            auto merge_input1 = Generate(
                ctx,
                [](size_t index) { return index * 2; },
                test_size);

            // odd numbers in 1..9999
            auto merge_input2 = merge_input1.Map(
                [](size_t i) { return i + 1; } );

            // merge
            auto merge_result = merge_input1.Merge(
                merge_input2, [](size_t a, size_t b) -> bool { return a < b; });

            // check if order was kept while merging. 
            auto res = merge_result.AllGather();

            ASSERT_EQ(test_size * 2, res.size());

            for (size_t i = 0; i != res.size() - 1; ++i) {
                ASSERT_TRUE(res[i] < res[i + 1]);
            }
        };

    thrill::api::RunLocalTests(start_func);
}
*/
