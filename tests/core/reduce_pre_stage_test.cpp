/*******************************************************************************
 * tests/core/reduce_pre_stage_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/core/reduce_pre_stage.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <utility>
#include <vector>

using namespace thrill;

struct MyStruct
{
    size_t key, value;

    bool operator < (const MyStruct& b) const { return key < b.key; }

    friend std::ostream& operator << (std::ostream& os, const MyStruct& c) {
        return os << '(' << c.key << ',' << c.value << ')';
    }
};

void TestAddMyStructByHash(Context& ctx) {
    static const size_t mod_size = 601;
    static const size_t test_size = mod_size * 100;

    auto key_ex = [](const MyStruct& in) {
                      return in.key % mod_size;
                  };

    auto red_fn = [](const MyStruct& in1, const MyStruct& in2) {
                      return MyStruct {
                                 in1.key, in1.value + in2.value
                      };
                  };

    // collect all items
    const size_t num_partitions = 13;

    std::vector<data::File> files;
    for (size_t i = 0; i < num_partitions; ++i)
        files.emplace_back(ctx.GetFile());

    std::vector<data::DynBlockWriter> emitters;
    for (size_t i = 0; i < num_partitions; ++i)
        emitters.emplace_back(files[i].GetDynWriter());

    // process items with stage
    using Stage = core::ReducePreBucketStage<
              MyStruct, size_t, MyStruct,
              decltype(key_ex), decltype(red_fn), true>;

    Stage stage(ctx,
                num_partitions,
                key_ex, red_fn, emitters,
                core::ReduceByHashKey<size_t>(),
                /* sentinel */ size_t(-1),
                /* neutral_element */ MyStruct(),
                /* limit_memory_bytes */ 1024 * 1024,
                /* limit_partition_fill_rate */ 0.6,
                /* bucket_rate */ 1.0);

    for (size_t i = 0; i < test_size; ++i) {
        stage.Insert(MyStruct { i, i / mod_size });
    }

    stage.Flush();
    stage.CloseAll();

    // collect items and check result
    std::vector<MyStruct> result;

    for (size_t i = 0; i < num_partitions; ++i) {
        data::File::Reader r = files[i].GetReader(/* consume */ true);
        while (r.HasNext())
            result.emplace_back(r.Next<MyStruct>());
    }

    std::sort(result.begin(), result.end());

    ASSERT_EQ(mod_size, result.size());

    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(i, result[i].key);
        ASSERT_EQ((test_size / mod_size) * ((test_size / mod_size) - 1) / 2,
                  result[i].value);
    }
}

TEST(ReducePreStage, AddMyStructByHash) {
    api::RunLocalSameThread(
        [](Context& ctx) { TestAddMyStructByHash(ctx); });
}

/******************************************************************************/

void TestAddMyStructByIndex(Context& ctx) {
    static const size_t mod_size = 601;
    static const size_t test_size = mod_size * 100;

    auto key_ex = [](const MyStruct& in) {
                      return in.key % mod_size;
                  };

    auto red_fn = [](const MyStruct& in1, const MyStruct& in2) {
                      return MyStruct {
                                 in1.key, in1.value + in2.value
                      };
                  };

    // collect all items
    const size_t num_partitions = 13;

    std::vector<data::File> files;
    for (size_t i = 0; i < num_partitions; ++i)
        files.emplace_back(ctx.GetFile());

    std::vector<data::DynBlockWriter> emitters;
    for (size_t i = 0; i < num_partitions; ++i)
        emitters.emplace_back(files[i].GetDynWriter());

    // process items with stage
    using Stage = core::ReducePreBucketStage<
              MyStruct, size_t, MyStruct,
              decltype(key_ex), decltype(red_fn), true,
              core::PreReduceByIndex<size_t> >;

    Stage stage(ctx,
                num_partitions,
                key_ex, red_fn, emitters,
                core::PreReduceByIndex<size_t>(mod_size),
                /* sentinel */ size_t(-1),
                /* neutral_element */ MyStruct(),
                /* limit_memory_bytes */ 1024 * 1024,
                /* limit_partition_fill_rate */ 0.6,
                /* bucket_rate */ 1.0);

    for (size_t i = 0; i < test_size; ++i) {
        stage.Insert(MyStruct { i, i / mod_size });
    }

    stage.Flush();
    stage.CloseAll();

    // collect items and check result - they must be in correct order!
    std::vector<MyStruct> result;

    for (size_t i = 0; i < num_partitions; ++i) {
        data::File::Reader r = files[i].GetReader(/* consume */ true);
        while (r.HasNext())
            result.emplace_back(r.Next<MyStruct>());
    }

    ASSERT_EQ(mod_size, result.size());

    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(i, result[i].key);
        ASSERT_EQ((test_size / mod_size) * ((test_size / mod_size) - 1) / 2,
                  result[i].value);
    }
}

TEST(ReducePreStage, AddMyStructByIndex) {
    api::RunLocalSameThread(
        [](Context& ctx) { TestAddMyStructByIndex(ctx); });
}

/******************************************************************************/
