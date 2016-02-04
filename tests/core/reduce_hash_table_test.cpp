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
#include <thrill/core/reduce_probing_hash_table.hpp>

#include <thrill/core/reduce_pre_table.hpp>

#include <gtest/gtest.h>

#include <vector>

using namespace thrill;

//! Table emitter implementation to collect output of a reduce hash table
template <typename Type>
class TableCollector : public std::vector<std::vector<Type> >
{
public:
    using Super = std::vector<std::vector<Type> >;

    TableCollector(size_t num_partitions)
        : Super(num_partitions) { }

    void Emit(const size_t& partition_id, const Type& p) {
        assert(partition_id < Super::size());
        Super::operator [] (partition_id).push_back(p);
    }
};

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

    using Collector = TableCollector<std::pair<size_t, MyStruct> >;

    Collector collector(13);

    using Table = core::ReduceProbingHashTable<
              MyStruct, size_t, MyStruct,
              decltype(key_ex), decltype(red_fn), Collector,
              false, core::PreReduceByHashKey<int> >;

    Table table(ctx, key_ex, red_fn, collector,
                /* num_partitions */ 13,
                /* limit_memory_bytes */ 1024 * 1024,
                /* limit_partition_fill_rate */ 0.6,
                /* bucket_rate */ 1.0);

    for (size_t i = 0; i < test_size; ++i) {
        table.Insert(MyStruct { i, i / mod_size });
    }

    table.FlushAll();

    // collect all items
    std::vector<MyStruct> result;

    for (size_t pi = 0; pi < collector.size(); ++pi) {
        const auto& partition = collector[pi];

        sLOG1 << "partition" << pi << ":" << partition.size() << ":";
        for (const auto& v : partition) {
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

TEST(ReduceHashTable, AddIntegers) {
    api::RunLocalSameThread(
        [](Context& ctx) { TestAddMyStructModulo(ctx); });
}

/******************************************************************************/
