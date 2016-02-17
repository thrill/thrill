/*******************************************************************************
 * tests/core/reduce_post_stage_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/core/reduce_by_hash_post_stage.hpp>
#include <thrill/core/reduce_by_index_post_stage.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
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

template <
    template <typename ValueType, typename Key, typename Value,
              typename KeyExtractor, typename ReduceFunction, typename Emitter,
              const bool SendPair = false,
              typename IndexFunction = core::ReduceByHash<Key>,
              typename ReduceStageConfig = core::DefaultReduceTableConfig,
              typename EqualToFunction = std::equal_to<Key> >
    class PostStage>
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

    using Stage = PostStage<
              MyStruct, size_t, MyStruct,
              decltype(key_ex), decltype(red_fn), decltype(emit_fn), false>;

    core::DefaultReduceTableConfig config;
    config.limit_memory_bytes_ = 64 * 1024;

    Stage stage(ctx, key_ex, red_fn, emit_fn,
                core::ReduceByHash<size_t>());
    stage.Initialize();

    for (size_t i = 0; i < test_size; ++i) {
        stage.Insert(MyStruct { i, i / mod_size });
    }

    stage.PushData(/* consume */ true);

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

TEST(ReduceHashStage, BucketAddMyStructByHash) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructByHash<core::ReducePostBucketStage>(ctx);
        });
}

TEST(ReduceHashStage, ProbingAddMyStructByHash) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructByHash<core::ReducePostProbingStage>(ctx);
        });
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
        core::ReduceIndexResult b
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

template <
    template <typename ValueType, typename Key, typename Value,
              typename KeyExtractor, typename ReduceFunction, typename Emitter,
              const bool SendPair = false,
              typename IndexFunction = core::ReduceByIndex<Key>,
              typename ReduceStageConfig = core::DefaultReduceTableConfig,
              typename EqualToFunction = std::equal_to<Key> >
    class PostStage>
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

    using Stage = PostStage<
              MyStruct, size_t, MyStruct,
              decltype(key_ex), decltype(red_fn), decltype(emit_fn), false>;

    core::DefaultReduceTableConfig config;
    config.limit_memory_bytes_ = 64 * 1024;

    Stage stage(ctx, key_ex, red_fn, emit_fn,
                core::ReduceByIndex<size_t>(0, mod_size),
                /* neutral_element */ MyStruct { 0, 0 });
    stage.Initialize();

    for (size_t i = 0; i < test_size; ++i) {
        stage.Insert(MyStruct { i, i / mod_size });
    }

    stage.PushData(/* consume */ true);

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

TEST(ReduceHashStage, BucketAddMyStructByIndex) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructByIndex<core::ReduceByIndexPostBucketStage>(ctx);
        });
}

TEST(ReduceHashStage, ProbingAddMyStructByIndex) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructByIndex<core::ReduceByIndexPostProbingStage>(ctx);
        });
}

/******************************************************************************/

template <
    template <typename ValueType, typename Key, typename Value,
              typename KeyExtractor, typename ReduceFunction, typename Emitter,
              const bool SendPair = false,
              typename IndexFunction = core::ReduceByIndex<Key>,
              typename ReduceStageConfig = core::DefaultReduceTableConfig,
              typename EqualToFunction = std::equal_to<Key> >
    class PostStage>
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

    using Stage = PostStage<
              MyStruct, size_t, MyStruct,
              decltype(key_ex), decltype(red_fn), decltype(emit_fn), false>;

    core::DefaultReduceTableConfig config;
    config.limit_memory_bytes_ = 64 * 1024;

    Stage stage(ctx, key_ex, red_fn, emit_fn,
                core::ReduceByIndex<size_t>(0, mod_size),
                /* neutral_element */ MyStruct { 0, 0 },
                config);
    stage.Initialize();

    for (size_t i = 0; i < test_size; ++i) {
        stage.Insert(MyStruct { i, i / mod_size });
    }

    stage.PushData(/* consume */ true);

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

TEST(ReduceHashStage, BucketAddMyStructByIndexWithHoles) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructByIndexWithHoles<core::ReduceByIndexPostBucketStage>(ctx);
        });
}

TEST(ReduceHashStage, ProbingAddMyStructByIndexWithHoles) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructByIndexWithHoles<core::ReduceByIndexPostProbingStage>(ctx);
        });
}

/******************************************************************************/
