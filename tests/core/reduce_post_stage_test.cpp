/*******************************************************************************
 * tests/core/reduce_post_stage_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/core/reduce_post_stage.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <utility>
#include <vector>

using namespace thrill;

struct MyStruct
{
    size_t key, value;

    bool operator < (const MyStruct& b) const { return key < b.key; }
};

void TestAddMyStructModulo(Context& ctx) {
    static const size_t test_size = 50000;
    static const size_t mod_size = 500;

    auto key_ex = [](const MyStruct& in) {
                      return in.key % mod_size;
                  };

    auto red_fn = [](const MyStruct& in1, const MyStruct& in2) {
                      return MyStruct {
                                 in1.key, in1.value + in2.value
                      };
                  };

    // collect all items
    std::vector<MyStruct> result;

    auto emit_fn = [&result](const MyStruct& in) {
                       result.emplace_back(in);
                   };

    using Stage = core::ReducePostBucketStage<
              MyStruct, size_t, MyStruct,
              decltype(key_ex), decltype(red_fn), false>;

    Stage stage(ctx, key_ex, red_fn, emit_fn,
                core::PostReduceByHashKey<size_t>(),
                /* sentinel */ size_t(),
                /* limit_memory_bytes */ 1024 * 1024,
                /* limit_partition_fill_rate */ 0.6,
                /* bucket_rate */ 1.0);

    for (size_t i = 0; i < test_size; ++i) {
        stage.Insert(MyStruct { i, i / mod_size });
    }

    stage.Flush();

    // check result
    std::sort(result.begin(), result.end());

    ASSERT_EQ(mod_size, result.size());

    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(i, result[i].key);
        ASSERT_EQ((test_size / mod_size) * ((test_size / mod_size) - 1) / 2,
                  result[i].value);
    }
}

TEST(ReduceHashStage, AddIntegers) {
    api::RunLocalSameThread(
        [](Context& ctx) { TestAddMyStructModulo(ctx); });
}

/******************************************************************************/
