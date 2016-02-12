/*******************************************************************************
 * tests/core/reduce_post_stage_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/core/reduce_by_index_post_stage.hpp>
#include <thrill/core/reduce_by_hash_post_stage.hpp>

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

/******************************************************************************/

static void TestAddMyStructByHash(Context& ctx) {
    static const bool debug = false;
    static const size_t mod_size = 601;
    static const size_t test_size = mod_size * 100;
    static const size_t val_size = test_size / mod_size;

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
                core::ReduceByHash<size_t>(),
                /* sentinel */ size_t(-1),
                /* limit_memory_bytes */ 64 * 1024,
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
        LOG << "result[" << i << "] = " << result[i] << " =? "
            << val_size * (val_size - 1) / 2;
    }

    for (size_t i = 0; i < result.size(); ++i) {
        LOG << "result[" << i << "] = " << result[i] << " =? "
            << val_size * (val_size - 1) / 2;
        ASSERT_EQ(i, result[i].key);
        ASSERT_EQ(val_size * (val_size - 1) / 2, result[i].value);
    }
}

TEST(ReduceHashStage, AddMyStructByHash) {
    api::RunLocalSameThread(
        [](Context& ctx) { TestAddMyStructByHash(ctx); });
}

/******************************************************************************/

TEST(ReduceHashStage, PostReduceByIndex) {
    static const bool debug = false;

    using IndexMap = core::ReduceByIndex<size_t>;

    IndexMap imap(0, 601);
    size_t num_partitions = 32;
    size_t num_buckets = 256;
    size_t num_buckets_per_partition = num_buckets / num_partitions;

    for (size_t key = 0; key < 601; ++key) {
        IndexMap::IndexResult b
            = imap(key,
                   num_partitions, num_buckets_per_partition, num_buckets);

        sLOG << "imap" << key << "->"
             << b.global_index << "part" << b.partition_id;

        die_unless(b.partition_id < num_partitions);
        die_unless(b.global_index < num_buckets);

        size_t inv = imap.inverse(b.global_index, num_buckets);

        sLOG << "inv" << b.global_index << "->" << inv;
        die_unless(inv <= key);
    }
}

/******************************************************************************/

static void TestAddMyStructByIndex(Context& ctx) {
    static const bool debug = false;
    static const size_t mod_size = 601;
    static const size_t test_size = mod_size * 100;
    static const size_t val_size = test_size / mod_size;

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

    using Stage = core::ReduceByIndexPostBucketStage<
              MyStruct, size_t, MyStruct,
              decltype(key_ex), decltype(red_fn), false>;

    Stage stage(ctx, key_ex, red_fn, emit_fn,
                core::ReduceByIndex<size_t>(0, mod_size),
                /* sentinel */ size_t(-1),
                /* neutral_element */ MyStruct { 0, 0 },
                /* limit_memory_bytes */ 64 * 1024,
                /* limit_partition_fill_rate */ 0.6,
                /* bucket_rate */ 1.0);

    for (size_t i = 0; i < test_size; ++i) {
        stage.Insert(MyStruct { i, i / mod_size });
    }

    stage.Flush();

    // check result
    ASSERT_EQ(mod_size, result.size());

    for (size_t i = 0; i < result.size(); ++i) {
        LOG << "result[" << i << "] = " << result[i] << " =? "
            << val_size * (val_size - 1) / 2;
    }

    for (size_t i = 0; i < result.size(); ++i) {
        LOG << "result[" << i << "] = " << result[i] << " =? "
            << val_size * (val_size - 1) / 2;
        ASSERT_EQ(i, result[i].key);
        ASSERT_EQ(val_size * (val_size - 1) / 2, result[i].value);
    }
}

TEST(ReduceHashStage, AddMyStructByIndex) {
    api::RunLocalSameThread(
        [](Context& ctx) { TestAddMyStructByIndex(ctx); });
}

/******************************************************************************/

static void TestAddMyStructByIndexWithHoles(Context& ctx) {
    static const bool debug = false;
    static const size_t mod_size = 600;
    static const size_t test_size = mod_size * 100;
    static const size_t val_size = test_size / mod_size;

    auto key_ex = [](const MyStruct& in) {
                      return (in.key * 2) % mod_size;
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

    using Stage = core::ReduceByIndexPostBucketStage<
              MyStruct, size_t, MyStruct,
              decltype(key_ex), decltype(red_fn), false>;

    Stage stage(ctx, key_ex, red_fn, emit_fn,
                core::ReduceByIndex<size_t>(0, mod_size),
                /* sentinel */ size_t(-1),
                /* neutral_element */ MyStruct { 0, 0 },
                /* limit_memory_bytes */ 64 * 1024,
                /* limit_partition_fill_rate */ 0.6,
                /* bucket_rate */ 1.0);

    for (size_t i = 0; i < test_size; ++i) {
        stage.Insert(MyStruct { i, i / mod_size });
    }

    stage.Flush();

    // check result
    ASSERT_EQ(mod_size, result.size());

    for (size_t i = 0; i < result.size(); ++i) {
        size_t correct = i % 2 == 0 ? val_size * (val_size - 1) : 0;

        LOG << "result[" << i << "] = " << result[i] << " =? " << correct;
    }

    for (size_t i = 0; i < result.size(); ++i) {
        size_t correct = i % 2 == 0 ? val_size * (val_size - 1) : 0;

        LOG << "result[" << i << "] = " << result[i] << " =? " << correct;

        ASSERT_EQ(i % 2 == 0 ? i / 2 : 0, result[i].key);
        ASSERT_EQ(correct, result[i].value);
    }
}

TEST(ReduceHashStage, AddMyStructByIndexWithHoles) {
    api::RunLocalSameThread(
        [](Context& ctx) { TestAddMyStructByIndexWithHoles(ctx); });
}

/******************************************************************************/
