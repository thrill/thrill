/*******************************************************************************
 * tests/core/reduce_hash_table_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_old_probing_hash_table.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>

#include <thrill/core/reduce_pre_stage.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <utility>
#include <vector>

static constexpr bool debug = false;

using namespace thrill;

//! Table emitter implementation to collect output of a reduce hash table
template <typename Type>
class TableCollector : public std::vector<std::vector<Type> >
{
public:
    using Super = std::vector<std::vector<Type> >;

    explicit TableCollector(size_t num_partitions)
        : Super(num_partitions) { }

    void Emit(const size_t& partition_id, const Type& p) {
        die_unless(partition_id < Super::size());
        Super::operator [] (partition_id).push_back(p);
    }
};

struct MyStruct {
    size_t key, value;

    MyStruct() = default;
    MyStruct(size_t k, size_t v) : key(k), value(v) { }

    bool operator < (const MyStruct& b) const { return key < b.key; }
};

struct MyReduceConfig : public core::DefaultReduceConfig {
    //! only for growing ProbingHashTable: items initially in a partition.
    static constexpr size_t initial_items_per_partition_ = 160000;
};

template <
    template <
        typename ValueType, typename Key, typename Value,
        typename KeyExtractor, typename ReduceFunction, typename Emitter,
        const bool VolatileKey,
        typename ReduceConfig = core::DefaultReduceConfig,
        typename IndexFunction = core::ReduceByHash<Key>,
        typename EqualToFunction = std::equal_to<Key> >
    class HashTable>
void TestAddMyStructModulo(Context& ctx) {
    static constexpr size_t test_size = 50000;
    static constexpr size_t mod_size = 500;

    auto key_ex = [](const MyStruct& in) {
                      return in.key % mod_size;
                  };

    auto red_fn = [](const MyStruct& in1, const MyStruct& in2) {
                      return MyStruct(in1.key, in1.value + in2.value);
                  };

    using Collector = TableCollector<std::pair<size_t, MyStruct> >;

    Collector collector(13);

    using Table = HashTable<
              MyStruct, size_t, MyStruct,
              decltype(key_ex), decltype(red_fn), Collector,
              /* VolatileKey */ false, MyReduceConfig>;

    Table table(ctx, 0, key_ex, red_fn, collector,
                /* num_partitions */ 13,
                typename Table::ReduceConfig(),
                /* immediate_flush */ true);
    table.Initialize(/* limit_memory_bytes */ 1024 * 1024);

    for (size_t i = 0; i < test_size; ++i) {
        table.Insert(MyStruct(i, i / mod_size));
    }

    table.FlushAll();

    // collect all items
    std::vector<MyStruct> result;

    for (size_t pi = 0; pi < collector.size(); ++pi) {
        const auto& partition = collector[pi];

        sLOG << "partition" << pi << ":" << partition.size() << ":";
        for (const auto & v : partition) {
            result.emplace_back(v.second);
        }
    }

    // check result
    std::sort(result.begin(), result.end());

    ASSERT_EQ(mod_size, result.size());

    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(i, result[i].key);
        ASSERT_EQ((test_size / mod_size) * ((test_size / mod_size) - 1) / 2,
                  result[i].value);
    }
}

TEST(ReduceHashTable, BucketAddIntegers) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructModulo<core::ReduceBucketHashTable>(ctx);
        });
}

TEST(ReduceHashTable, ProbingAddIntegers) {
    api::RunLocalSameThread(
        [](Context& ctx) {
            TestAddMyStructModulo<core::ReduceProbingHashTable>(ctx);
        });
}

/******************************************************************************/
