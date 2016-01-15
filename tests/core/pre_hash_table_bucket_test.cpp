/*******************************************************************************
 * tests/core/pre_hash_table_bucket_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/context.hpp>
#include <thrill/core/reduce_post_bucket_table.hpp>
#include <thrill/core/reduce_pre_bucket_table.hpp>
#include <thrill/data/file.hpp>
#include <thrill/net/manager.hpp>

#include <algorithm>
#include <functional>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;

using IntPair = std::pair<int, int>;
using StringPairPair = std::pair<std::string, std::pair<std::string, int> >;
using StringPair = std::pair<std::string, int>;

struct PreTable : public::testing::Test { };

struct MyStruct
{
    size_t key;
    int    count;
};

using MyPair = std::pair<int, MyStruct>;

template <typename Key, typename HashFunction = std::hash<Key> >
class CustomKeyHashFunction
    : public core::PreProbingReduceByHashKey<int>
{
public:
    struct IndexResult {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;

        IndexResult(size_t p_id, size_t g_id) {
            partition_id = p_id;
            global_index = g_id;
        }
    };

    explicit CustomKeyHashFunction(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function)
    { }

    IndexResult
    operator () (const Key& k,
                 const size_t& num_frames,
                 const size_t& num_buckets_per_frame,
                 const size_t& num_buckets_per_table,
                 const size_t& offset) const {

        size_t global_index = 0;
        size_t partition_id = 0;

        (void)k;
        (void)num_frames;
        (void)num_buckets_per_frame;
        (void)num_buckets_per_table;
        (void)offset;

        return IndexResult(partition_id, global_index);
    }

private:
    HashFunction hash_function_;
};

TEST_F(PreTable, CustomHashFunction) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            data::BlockPool block_pool;
            data::File output(block_pool, 0);
            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output.GetDynWriter());

            CustomKeyHashFunction<int> cust_hash;

            core::ReducePreTable<int, int, int, decltype(key_ex), decltype(red_fn), true,
                                 core::PostBucketReduceFlush<int, int, decltype(red_fn)>, CustomKeyHashFunction<int> >
            table(ctx, 1, key_ex, red_fn, writers, cust_hash,
                  core::PostBucketReduceFlush<int, int, decltype(red_fn)>(red_fn),
                  int(), -1, 1024 * 16, 0.001, 1.0);

            for (int i = 0; i < 16; i++) {
                table.Insert(i);
            }

            table.Flush();

            auto it1 = output.GetKeepReader();
            int c = 0;
            while (it1.HasNext()) {
                it1.Next<int>();
                c++;
            }

            ASSERT_EQ(16, c);
        };
    api::RunLocalSameThread(start_func);
}

TEST_F(PreTable, AddIntegers) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            data::BlockPool block_pool;
            data::File output(block_pool, 0);
            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output.GetDynWriter());

            core::ReducePreTable<int, int, int, decltype(key_ex), decltype(red_fn), true,
                                 core::PostBucketReduceFlush<int, int, decltype(red_fn)>, CustomKeyHashFunction<int> >
            table(ctx, 1, key_ex, red_fn, writers);

            table.Insert(1);
            table.Insert(2);
            table.Insert(3);

            ASSERT_EQ(3u, table.NumItemsPerTable());

            table.Insert(2);

            ASSERT_EQ(3u, table.NumItemsPerTable());
        };
    api::RunLocalSameThread(start_func);
}

TEST_F(PreTable, CreateEmptyTable) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            data::BlockPool block_pool;
            data::File output(block_pool, 0);
            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output.GetDynWriter());

            core::ReducePreTable<int, int, int, decltype(key_ex), decltype(red_fn), true>
            table(ctx, 1, key_ex, red_fn, writers);

            table.Insert(1);
            table.Insert(2);
            table.Insert(3);

            ASSERT_EQ(3u, table.NumItemsPerTable());

            table.Insert(2);

            ASSERT_EQ(3u, table.NumItemsPerTable());
        };
    api::RunLocalSameThread(start_func);
}

TEST_F(PreTable, PopIntegers) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            auto key_ex = [](int in) {
                              return in;
                          };

            data::BlockPool block_pool;
            data::File output(block_pool, 0);
            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output.GetDynWriter());

            const size_t TargetBlockSize = 8 * 8;
            const size_t bucket_block_size = sizeof(core::ReducePreTable<int, int, int, decltype(key_ex), decltype(red_fn), true,
                                                                         core::PostBucketReduceFlush<int, int, decltype(red_fn)>, core::PreProbingReduceByHashKey<int>,
                                                                         std::equal_to<int>, TargetBlockSize>::BucketBlock);

            core::ReducePreTable<int, int, int, decltype(key_ex), decltype(red_fn), true,
                                 core::PostBucketReduceFlush<int, int, decltype(red_fn)>,
                                 core::PreProbingReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(ctx, 1, key_ex, red_fn, writers, core::PreProbingReduceByHashKey<int>(),
                  core::PostBucketReduceFlush<int, int, decltype(red_fn)>(red_fn), -1, bucket_block_size * 2, 0.0, 1.0);

            table.Insert(0);
            table.Insert(1);
            table.Insert(2);
            table.Insert(3);
            table.Insert(4);
            table.Insert(5);
            table.Insert(6);
            table.Insert(7);

            // ASSERT_EQ(8u, table.NumItemsPerTable());

            table.Insert(9);

            // ASSERT_EQ(1u, table.NumItemsPerTable());
        };
    api::RunLocalSameThread(start_func);
}

// Manually flush all items in table,
// no size constraint, one partition
TEST_F(PreTable, FlushIntegersManuallyOnePartition) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            data::BlockPool block_pool;
            data::File output(block_pool, 0);
            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output.GetDynWriter());

            core::ReducePreTable<int, int, int, decltype(key_ex), decltype(red_fn), true>
            table(ctx, 1, key_ex, red_fn, writers, core::PreProbingReduceByHashKey<int>(),
                  core::PostBucketReduceFlush<int, int, decltype(red_fn)>(red_fn), -1, 8 * 1024, 0.001, 1.0);

            table.Insert(0);
            table.Insert(1);
            table.Insert(2);
            table.Insert(3);
            table.Insert(4);

            ASSERT_EQ(5u, table.NumItemsPerTable());

            table.Flush();
            ASSERT_EQ(0u, table.NumItemsPerTable());

            auto it = output.GetKeepReader();
            int c = 0;
            while (it.HasNext()) {
                it.Next<int>();
                c++;
            }

            ASSERT_EQ(5, c);
        };
    api::RunLocalSameThread(start_func);
}

// Manually flush all items in table,
// no size constraint, two partitions
TEST_F(PreTable, FlushIntegersManuallyTwoPartitions) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            data::BlockPool block_pool;
            data::File output1(block_pool, 0), output2(block_pool, 0);
            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output1.GetDynWriter());
            writers.emplace_back(output2.GetDynWriter());

            core::ReducePreTable<int, int, int, decltype(key_ex), decltype(red_fn), true>
            table(ctx, 2, key_ex, red_fn, writers, core::PreProbingReduceByHashKey<int>(),
                  core::PostBucketReduceFlush<int, int, decltype(red_fn)>(red_fn), -1, 8 * 1024, 0.001, 1.0);

            table.Insert(0);
            table.Insert(1);
            table.Insert(2);
            table.Insert(3);
            table.Insert(4);

            ASSERT_EQ(5u, table.NumItemsPerTable());

            table.Flush();
            ASSERT_EQ(0u, table.NumItemsPerTable());

            auto it1 = output1.GetKeepReader();
            int c1 = 0;
            while (it1.HasNext()) {
                it1.Next<int>();
                c1++;
            }

            auto it2 = output2.GetKeepReader();
            int c2 = 0;
            while (it2.HasNext()) {
                it2.Next<int>();
                c2++;
            }

            ASSERT_EQ(5u, c1 + c2);
        };
    api::RunLocalSameThread(start_func);
}

// Partial flush of items in table due to
// max table size constraint, one partition
TEST_F(PreTable, FlushIntegersPartiallyOnePartition) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            data::BlockPool block_pool;
            data::File output(block_pool, 0);
            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output.GetDynWriter());

            const size_t TargetBlockSize = 8 * 8;
            const size_t bucket_block_size = sizeof(core::ReducePreTable<int, int, int, decltype(key_ex), decltype(red_fn), true,
                                                                         core::PostBucketReduceFlush<int, int, decltype(red_fn)>,
                                                                         core::PreProbingReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>::BucketBlock);

            core::ReducePreTable<int, int, int, decltype(key_ex), decltype(red_fn), true,
                                 core::PostBucketReduceFlush<int, int, decltype(red_fn)>,
                                 core::PreProbingReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(ctx, 1, key_ex, red_fn, writers, core::PreProbingReduceByHashKey<int>(),
                  core::PostBucketReduceFlush<int, int, decltype(red_fn)>(red_fn), -1, -1, bucket_block_size * 2, 0.5, 0.5, std::equal_to<int>(), 0.0);

            table.Insert(0);
            table.Insert(1);
            table.Insert(2);
            table.Insert(3);
            table.Insert(4);
            table.Insert(5);
            table.Insert(6);
            table.Insert(7);

            ASSERT_EQ(8u, table.NumItemsPerTable());

            table.Insert(8);

            auto it = output.GetKeepReader();
            int c = 0;
            while (it.HasNext()) {
                it.Next<int>();
                c++;
            }

            ASSERT_EQ(8, c);
        };
    api::RunLocalSameThread(start_func);
}

//// Partial flush of items in table due to
//// max table size constraint, two partitions
TEST_F(PreTable, FlushIntegersPartiallyTwoPartitions) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            data::BlockPool block_pool;
            data::File output1(block_pool, 0), output2(block_pool, 0);

            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output1.GetDynWriter());
            writers.emplace_back(output2.GetDynWriter());

            core::ReducePreTable<int, int, int, decltype(key_ex), decltype(red_fn), true>
            table(ctx, 2, key_ex, red_fn, writers, core::PreProbingReduceByHashKey<int>(),
                  core::PostBucketReduceFlush<int, int, decltype(red_fn)>(red_fn), -1, 8 * 1024, 0.001, 1.0);

            table.Insert(0);
            table.Insert(1);
            table.Insert(2);
            table.Insert(3);

            ASSERT_EQ(4u, table.NumItemsPerTable());

            table.Insert(4);
            table.Flush();

            auto it1 = output1.GetKeepReader();
            int c1 = 0;
            while (it1.HasNext()) {
                it1.Next<int>();
                c1++;
            }

            table.Flush();

            auto it2 = output2.GetKeepReader();
            int c2 = 0;
            while (it2.HasNext()) {
                it2.Next<int>();
                c2++;
            }

            ASSERT_EQ(5u, c1 + c2);
            ASSERT_EQ(0u, table.NumItemsPerTable());
        };
    api::RunLocalSameThread(start_func);
}

TEST_F(PreTable, ComplexType) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](StringPair in) {
                              return in.first;
                          };

            auto red_fn = [](StringPair in1, StringPair in2) {
                              return std::make_pair(in1.first, in1.second + in2.second);
                          };

            data::BlockPool block_pool;
            data::File output(block_pool, 0);
            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output.GetDynWriter());

            StringPair sp;

            core::ReducePreTable<StringPair, std::string, StringPair, decltype(key_ex), decltype(red_fn), true>
            table(ctx, 1, key_ex, red_fn, writers, core::PreProbingReduceByHashKey<std::string>(),
                  core::PostBucketReduceFlush<std::string, StringPair,
                                              decltype(red_fn)>(red_fn), "", sp, 16 * 1024, 0.001, 0.5);

            table.Insert(std::make_pair("hallo", 1));
            table.Insert(std::make_pair("hello", 2));
            table.Insert(std::make_pair("bonjour", 3));

            ASSERT_EQ(3u, table.NumItemsPerTable());

            table.Insert(std::make_pair("hello", 5));

            ASSERT_EQ(3u, table.NumItemsPerTable());

            table.Insert(std::make_pair("baguette", 42));

            table.Flush();

            ASSERT_EQ(0u, table.NumItemsPerTable());
        };
    api::RunLocalSameThread(start_func);
}

TEST_F(PreTable, MultipleWorkers) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            data::BlockPool block_pool;
            data::File output1(block_pool, 0), output2(block_pool, 0);

            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output1.GetDynWriter());
            writers.emplace_back(output2.GetDynWriter());

            const size_t TargetBlockSize = 8 * 8;
            const size_t bucket_block_size = sizeof(core::ReducePreTable<int, int, int, decltype(key_ex), decltype(red_fn), true,
                                                                         core::PostBucketReduceFlush<int, int, decltype(red_fn)>,
                                                                         core::PreProbingReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>::BucketBlock);

            core::ReducePreTable<int, int, int, decltype(key_ex), decltype(red_fn), true,
                                 core::PostBucketReduceFlush<int, int, decltype(red_fn)>,
                                 core::PreProbingReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(ctx, 2, key_ex, red_fn, writers, core::PreProbingReduceByHashKey<int>(),
                  core::PostBucketReduceFlush<int, int,
                                              decltype(red_fn)>(red_fn), -1, bucket_block_size * 2, 1.0, 0.5);

            ASSERT_EQ(0u, table.NumItemsPerTable());

            for (int i = 0; i < 6; i++) {
                table.Insert(i * 35001);
            }

            ASSERT_LE(table.NumItemsPerTable(), 6u);
            ASSERT_GT(table.NumItemsPerTable(), 0u);
        };
    api::RunLocalSameThread(start_func);
}

// Insert several items with same key and test application of local reduce
TEST_F(PreTable, InsertManyIntsAndTestReduce1) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](const MyStruct& in) {
                              return in.key % 500;
                          };

            auto red_fn = [](const MyStruct& in1, const MyStruct& in2) {
                              return MyStruct {
                                         in1.key, in1.count + in2.count
                              };
                          };

            size_t total_sum = 0, total_count = 0;

            data::BlockPool block_pool;
            data::File output(block_pool, 0);
            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output.GetDynWriter());

            size_t nitems = 1 * 1024 * 1024;
            MyStruct sp;

            // Hashtable with smaller block size for testing.
            core::ReducePreTable<MyStruct, size_t, MyStruct, decltype(key_ex), decltype(red_fn), true>
            table(ctx, 1, key_ex, red_fn, writers, core::PreProbingReduceByHashKey<size_t>(),
                  core::PostBucketReduceFlush<size_t, MyStruct, decltype(red_fn)>(red_fn),
                  -1, sp, nitems * 16, 0.001, 0.5);

            // insert lots of items
            for (size_t i = 0; i != nitems; ++i) {
                table.Insert(MyStruct { i, 1 });
            }

            table.Flush();

            auto it1 = output.GetKeepReader();
            while (it1.HasNext()) {
                auto n = it1.Next<MyStruct>();
                total_count++;
                total_sum += n.count;
            }

            // actually check that the reduction worked
            ASSERT_EQ(500u, total_count);
            ASSERT_EQ(nitems, total_sum);
        };
    api::RunLocalSameThread(start_func);
}

TEST_F(PreTable, InsertManyIntsAndTestReduce2) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](const MyStruct& in) {
                              return static_cast<int>(in.key);
                          };

            auto red_fn = [](const MyStruct& in1, const MyStruct& in2) {
                              return MyStruct {
                                         in1.key, in1.count + in2.count
                              };
                          };

            data::BlockPool block_pool;
            data::File output(block_pool, 0);
            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output.GetDynWriter());

            MyStruct sp;

            const size_t nitems_per_key = 10;
            const size_t nitems = 1 * 4 * 1024;

            const size_t TargetBlockSize = nitems * sizeof(MyStruct);
            const size_t bucket_block_size = sizeof(core::ReducePreTable<MyStruct, int, MyStruct, decltype(key_ex), decltype(red_fn), true,
                                                                         core::PostBucketReduceFlush<int, MyStruct, decltype(red_fn)>,
                                                                         core::PreProbingReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>::BucketBlock);

            core::ReducePreTable<MyStruct, int, MyStruct, decltype(key_ex), decltype(red_fn), true,
                                 core::PostBucketReduceFlush<int, MyStruct, decltype(red_fn)>,
                                 core::PreProbingReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(ctx, 1, key_ex, red_fn, writers, core::PreProbingReduceByHashKey<int>(),
                  core::PostBucketReduceFlush<
                      int, MyStruct, decltype(red_fn)>(red_fn), -1, sp, bucket_block_size * bucket_block_size, 1.0, 1.0);

            // insert lots of items
            size_t sum = 0;
            for (size_t i = 0; i != nitems_per_key; ++i) {
                sum += i;
                for (size_t j = 0; j != nitems; ++j) {
                    table.Insert(MyStruct { j, static_cast<int>(i) });
                }
            }

            ASSERT_EQ(nitems, table.NumItemsPerTable());

            table.Flush();

            ASSERT_EQ(0u, table.NumItemsPerTable());

            auto it1 = output.GetKeepReader();
            while (it1.HasNext()) {
                auto n = it1.Next<MyStruct>();
                ASSERT_EQ(sum, n.count);
            }
        };
    api::RunLocalSameThread(start_func);
}

void randomStr(std::string& s, const int len) {
    s.resize(len);

    static const char alphanum[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

TEST_F(PreTable, InsertManyStringItemsAndTestReduce) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](StringPair in) {
                              return in.first;
                          };

            auto red_fn = [](StringPair in1, StringPair in2) {
                              return std::make_pair(in1.first, in1.second + in2.second);
                          };

            data::BlockPool block_pool;
            data::File output(block_pool, 0);
            std::vector<data::File::DynWriter> writers;
            writers.emplace_back(output.GetDynWriter());

            const size_t TargetBlockSize = 16 * 1024;
            StringPair sp;

            const size_t nitems_per_key = 10;
            const size_t nitems = core::ReducePreTable<StringPair, std::string, StringPair, decltype(key_ex), decltype(red_fn), true,
                                                       core::PostBucketReduceFlush<std::string, StringPair, decltype(red_fn)>,
                                                       core::PreProbingReduceByHashKey<std::string>, std::equal_to<std::string>, TargetBlockSize>::block_size_;

            core::ReducePreTable<StringPair, std::string, StringPair, decltype(key_ex), decltype(red_fn), true,
                                 core::PostBucketReduceFlush<std::string, StringPair, decltype(red_fn)>,
                                 core::PreProbingReduceByHashKey<std::string>, std::equal_to<std::string>, TargetBlockSize>
            table(ctx, 1, key_ex, red_fn, writers, core::PreProbingReduceByHashKey<std::string>(),
                  core::PostBucketReduceFlush<
                      std::string, StringPair, decltype(red_fn)>(red_fn), "", sp, TargetBlockSize * 2, 0.0, 1.0);

            // insert lots of items
            size_t sum = 0;
            for (size_t j = 0; j != nitems; ++j) {
                sum = 0;
                std::string str;
                randomStr(str, 128);
                for (size_t i = 0; i != nitems_per_key; ++i) {
                    sum += i;
                    table.Insert(std::make_pair(str, i));
                }
            }

            ASSERT_EQ(nitems, table.NumItemsPerTable());

            table.Flush();

            ASSERT_EQ(0u, table.NumItemsPerTable());

            auto it1 = output.GetKeepReader();
            while (it1.HasNext()) {
                auto n = it1.Next<StringPair>();
                ASSERT_EQ(sum, n.second);
            }
        };
    api::RunLocalSameThread(start_func);
}

/******************************************************************************/
