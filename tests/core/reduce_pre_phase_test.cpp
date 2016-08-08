/*******************************************************************************
 * tests/core/reduce_pre_phase_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/core/reduce_pre_phase.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <utility>
#include <vector>

using namespace thrill;

struct MyStruct {
    size_t key, value;

    bool operator < (const MyStruct& b) const { return key < b.key; }

    friend std::ostream& operator << (std::ostream& os, const MyStruct& c) {
        return os << '(' << c.key << ',' << c.value << ')';
    }
};

template <core::ReduceTableImpl table_impl>
struct MyReduceConfig : public core::DefaultReduceConfig {
    //! only for growing ProbingHashTable: items initially in a partition.
    static constexpr size_t                initial_items_per_partition_ = 160000;

    //! select the hash table in the reduce phase by enum
    static constexpr core::ReduceTableImpl table_impl_ = table_impl;
};

/******************************************************************************/

template <core::ReduceTableImpl table_impl>
static void TestAddMyStructByHash(Context& ctx) {
    static constexpr size_t mod_size = 601;
    static constexpr size_t test_size = mod_size * 100;

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
        files.emplace_back(ctx.GetFile(nullptr));

    std::vector<data::DynBlockWriter> emitters;
    for (size_t i = 0; i < num_partitions; ++i)
        emitters.emplace_back(files[i].GetDynWriter());

    // process items with phase
    using Phase = core::ReducePrePhase<
              MyStruct, size_t, MyStruct,
              decltype(key_ex), decltype(red_fn),
              /* VolatileKey */ false,
              MyReduceConfig<table_impl> >;

    Phase phase(ctx, 0, num_partitions, key_ex, red_fn, emitters);

    phase.Initialize(/* limit_memory_bytes */ 1024 * 1024);

    for (size_t i = 0; i < test_size; ++i) {
        phase.Insert(MyStruct { i, i / mod_size });
    }

    phase.FlushAll();
    phase.CloseAll();

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

TEST(ReducePrePhase, BucketAddMyStructByHash) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructByHash<core::ReduceTableImpl::BUCKET>(ctx);
        });
}

TEST(ReducePrePhase, OldProbingAddMyStructByHash) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructByHash<core::ReduceTableImpl::OLD_PROBING>(ctx);
        });
}

TEST(ReducePrePhase, ProbingAddMyStructByHash) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructByHash<core::ReduceTableImpl::PROBING>(ctx);
        });
}

/******************************************************************************/

template <core::ReduceTableImpl table_impl>
static void TestAddMyStructByIndex(Context& ctx) {
    static constexpr size_t mod_size = 601;
    static constexpr size_t test_size = mod_size * 100;

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
        files.emplace_back(ctx.GetFile(nullptr));

    std::vector<data::DynBlockWriter> emitters;
    for (size_t i = 0; i < num_partitions; ++i)
        emitters.emplace_back(files[i].GetDynWriter());

    // process items with phase
    using Phase = core::ReducePrePhase<
              MyStruct, size_t, MyStruct,
              decltype(key_ex), decltype(red_fn),
              /* VolatileKey */ false,
              MyReduceConfig<table_impl>,
              core::ReduceByIndex<size_t> >;

    Phase phase(ctx, 0,
                num_partitions,
                key_ex, red_fn, emitters,
                typename Phase::ReduceConfig(),
                core::ReduceByIndex<size_t>(0, mod_size));

    phase.Initialize(/* limit_memory_bytes */ 1024 * 1024);

    for (size_t i = 0; i < test_size; ++i) {
        phase.Insert(MyStruct { i, i / mod_size });
    }

    phase.FlushAll();
    phase.CloseAll();

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

TEST(ReducePrePhase, BucketAddMyStructByIndex) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructByIndex<core::ReduceTableImpl::BUCKET>(ctx);
        });
}

TEST(ReducePrePhase, OldProbingAddMyStructByIndex) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructByIndex<core::ReduceTableImpl::OLD_PROBING>(ctx);
        });
}

TEST(ReducePrePhase, ProbingAddMyStructByIndex) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructByIndex<core::ReduceTableImpl::PROBING>(ctx);
        });
}

/******************************************************************************/
