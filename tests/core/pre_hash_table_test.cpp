/*******************************************************************************
 * tests/core/pre_hash_table_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_pre_table.hpp>

#include "gtest/gtest.h"

using namespace c7a::data;
using namespace c7a::net;
using IntPair = std::pair<int, int>;
using StringPairPair = std::pair<std::string, std::pair<std::string, int> >;
using StringPair = std::pair<std::string, int>;

struct PreTable : public::testing::Test { };

struct MyStruct
{
    int key;
    int count;

    // only initializing constructor, no default construction possible.
    explicit MyStruct(int k, int c) : key(k), count(c)
    { }
};

using MyPair = std::pair<int, MyStruct>;

namespace c7a {
namespace data {
namespace serializers {

template <>
struct Impl<MyStruct>{
    static std::string Serialize(const MyStruct& s) {
        std::size_t len = 2 * sizeof(int);
        char result[len];
        std::memcpy(result, &(s.key), sizeof(int));
        std::memcpy(result + sizeof(int), &(s.count), sizeof(int));
        return std::string(result, len);
    }

    static MyStruct Deserialize(const std::string& x) {
        int i, j;
        std::memcpy(&i, x.c_str(), sizeof(int));
        std::memcpy(&j, x.substr(sizeof(int), 2 * sizeof(int)).c_str(), sizeof(int));
        return MyStruct(i, j);
    }
};

} // namespace serializers

template <typename Archive>
struct Serializer<Archive, MyStruct>
{
    static void serialize(const MyStruct& x, Archive& a) {
        Serializer<Archive, int>::serialize(x.key, a);
        Serializer<Archive, int>::serialize(x.count, a);
    }
    static MyStruct deserialize(Archive& a) {
        int key = Serializer<Archive, int>::deserialize(a);
        int count = Serializer<Archive, int>::deserialize(a);
        return MyStruct(key, count);
    }
    static const bool fixed_size = (Serializer<Archive, int>::fixed_size &&
                                    Serializer<Archive, int>::fixed_size);
};

} // namespace data
} // namespace c7a

TEST_F(PreTable, CustomHashFunction) {

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    using HashTable = typename c7a::core::ReducePreTable<
              decltype(key_ex), decltype(red_fn), File::Writer>;

    auto hash_function = [](int key, HashTable*) {

                             size_t global_index = key / 2;
                             size_t partition_id = 0;
                             size_t partition_offset = key / 2;

                             return HashTable::hash_result(partition_id, partition_offset, global_index);
                         };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output);

    HashTable table(1, 8, 2, 20, 100, key_ex, red_fn, writers, hash_function);

    for (int i = 0; i < 16; i++) {
        table.Insert(std::move(i));
    }

    table.Flush();

    auto it1 = output.GetReader();
    int c = 0;
    while (it1.HasNext()) {
        IntPair keyvalue = it1.Next<IntPair>();
        ASSERT_EQ(keyvalue.first, keyvalue.second);
        c++;
    }

    ASSERT_EQ(16, c);
}

TEST_F(PreTable, AddIntegers) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, key_ex, red_fn, writers);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3u, table.Size());

    table.Insert(2);

    ASSERT_EQ(3u, table.Size());
}

TEST_F(PreTable, CreateEmptyTable) {
    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, key_ex, red_fn, writers);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3u, table.Size());

    table.Insert(2);

    ASSERT_EQ(3u, table.Size());
}

TEST_F(PreTable, PopIntegers) {
    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    auto key_ex = [](int in) { return in; };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, key_ex, red_fn, writers);

    table.SetMaxSize(3);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(0u, table.Size());

    table.Insert(1);

    ASSERT_EQ(1u, table.Size());
}

// Manually flush all items in table,
// no size constraint, one partition
TEST_F(PreTable, FlushIntegersManuallyOnePartition) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, 10, 2, 10, 10, key_ex, red_fn, writers);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5u, table.Size());

    table.Flush();
    ASSERT_EQ(0u, table.Size());

    auto it = output.GetReader();
    int c = 0;
    while (it.HasNext()) {
        it.Next<IntPair>();
        c++;
    }

    ASSERT_EQ(5, c);
}

// Manually flush all items in table,
// no size constraint, two partitions
TEST_F(PreTable, FlushIntegersManuallyTwoPartitions) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output1, output2;
    std::vector<File::Writer> writers;
    writers.emplace_back(output1);
    writers.emplace_back(output2);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(2, 5, 2, 10, 10, key_ex, red_fn, writers);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5u, table.Size());

    table.Flush();
    ASSERT_EQ(0u, table.Size());

    auto it1 = output1.GetReader();
    int c1 = 0;
    while (it1.HasNext()) {
        it1.Next<IntPair>();
        c1++;
    }

    ASSERT_EQ(3, c1);

    auto it2 = output2.GetReader();
    int c2 = 0;
    while (it2.HasNext()) {
        it2.Next<IntPair>();
        c2++;
    }

    ASSERT_EQ(2, c2);
}

// Partial flush of items in table due to
// max table size constraint, one partition
TEST_F(PreTable, FlushIntegersPartiallyOnePartition) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, 10, 2, 10, 4, key_ex, red_fn, writers);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(4u, table.Size());

    table.Insert(4);

    auto it = output.GetReader();
    int c = 0;
    while (it.HasNext()) {
        it.Next<IntPair>();
        c++;
    }

    ASSERT_EQ(5, c);
}

//// Partial flush of items in table due to
//// max table size constraint, two partitions
TEST_F(PreTable, FlushIntegersPartiallyTwoPartitions) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output1, output2;

    std::vector<File::Writer> writers;
    writers.emplace_back(output1);
    writers.emplace_back(output2);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(2, 5, 2, 10, 4, key_ex, red_fn, writers);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(4u, table.Size());

    table.Insert(4);
    table.Flush();

    auto it1 = output1.GetReader();
    int c1 = 0;
    while (it1.HasNext()) {
        it1.Next<IntPair>();
        c1++;
    }

    ASSERT_EQ(3, c1);
    table.Flush();

    auto it2 = output2.GetReader();
    int c2 = 0;
    while (it2.HasNext()) {
        it2.Next<IntPair>();
        c2++;
    }

    ASSERT_EQ(2, c2);
    ASSERT_EQ(0u, table.Size());
}

TEST_F(PreTable, ComplexType) {

    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, 2, 2, 10, 3, key_ex, red_fn, writers);

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));
    table.Insert(std::make_pair("bonjour", 3));

    ASSERT_EQ(3u, table.Size());

    table.Insert(std::make_pair("hello", 5));

    ASSERT_EQ(3u, table.Size());

    table.Insert(std::make_pair("baguette", 42));

    ASSERT_EQ(0u, table.Size());
}

TEST_F(PreTable, MultipleWorkers) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output1, output2;

    std::vector<File::Writer> writers;
    writers.emplace_back(output1);
    writers.emplace_back(output2);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(2, key_ex, red_fn, writers);

    ASSERT_EQ(0u, table.Size());
    table.SetMaxSize(5);

    for (int i = 0; i < 6; i++) {
        table.Insert(i * 35001);
    }

    ASSERT_LE(table.Size(), 3u);
    ASSERT_GT(table.Size(), 0u);
}

// Resize due to max bucket size reached. Set max items per bucket to 1,
// then add 2 items with different key, but having same hash value, one partition
TEST_F(PreTable, ResizeOnePartition) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output;
    {
        std::vector<File::Writer> writers;
        writers.emplace_back(output);

        c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), File::Writer>
        table(1, 1, 10, 1, 10, key_ex, red_fn, writers);

        table.Insert(1);

        ASSERT_EQ(1u, table.NumBuckets());
        ASSERT_EQ(1u, table.PartitionSize(0));
        ASSERT_EQ(1u, table.Size());

        table.Insert(2); // Resize happens here

        ASSERT_EQ(10u, table.NumBuckets());
        ASSERT_EQ(2u, table.PartitionSize(0));
        ASSERT_EQ(2u, table.Size());

        table.Flush();
    }

    auto it1 = output.GetReader();
    int c = 0;
    while (it1.HasNext()) {
        it1.Next<IntPair>();
        c++;
    }

    ASSERT_EQ(2, c);
}

// Resize due to max bucket size reached. Set max items per bucket to 1,
// then add 2 items with different key, but having same hash value, two partitions
// Check that same items are in same partition after resize
TEST_F(PreTable, ResizeTwoPartitions) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output1, output2;

    std::vector<File::Writer> writers;
    writers.emplace_back(output1);
    writers.emplace_back(output2);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(2, 1, 10, 1, 10, key_ex, red_fn, writers);

    ASSERT_EQ(0u, table.Size());
    ASSERT_EQ(2u, table.NumBuckets());
    ASSERT_EQ(0u, table.PartitionSize(0));
    ASSERT_EQ(0u, table.PartitionSize(1));

    table.Insert(1);
    table.Insert(2);

    ASSERT_EQ(2u, table.Size());
    ASSERT_EQ(2u, table.NumBuckets());
    ASSERT_EQ(1u, table.PartitionSize(0));
    ASSERT_EQ(1u, table.PartitionSize(1));

    table.Insert(3); // Resize happens here

    ASSERT_EQ(3u, table.Size());
    ASSERT_EQ(20u, table.NumBuckets());
    ASSERT_EQ(3u, table.PartitionSize(0) + table.PartitionSize(1));
}

TEST_F(PreTable, ResizeAndTestPartitionsHaveSameKeys) {
    auto key_ex = [](const MyStruct& in) {
                      return in.key;
                  };

    auto red_fn = [](const MyStruct& in1, const MyStruct& in2) {
                      return MyStruct(in1.key, in1.count + in2.count);
                  };

    size_t num_partitions = 3;
    size_t num_buckets_init_scale = 2;
    size_t bucket_size = 1 * 1024;
    size_t nitems = bucket_size + (num_partitions * num_buckets_init_scale * bucket_size);

    std::vector<File> files(num_partitions);
    std::vector<File::Writer> writers;
    for (size_t i = 0; i != num_partitions; ++i) {
        writers.emplace_back(files[i]);
    }

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn),
                              File::Writer, 16*1024>
    table(num_partitions, num_buckets_init_scale, 10, bucket_size,
          nitems, key_ex, red_fn, writers);

    for (size_t i = 0; i != num_partitions; ++i) {
        ASSERT_EQ(0u, table.PartitionSize(i));
    }
    ASSERT_EQ(num_partitions * num_buckets_init_scale, table.NumBuckets());
    ASSERT_EQ(0u, table.Size());

    // insert as many items which DO NOT lead to bucket overflow
    for (size_t i = 0; i != bucket_size; ++i) {
        table.Insert(MyStruct(i, 0));
    }

    ASSERT_EQ(num_partitions * num_buckets_init_scale, table.NumBuckets());
    ASSERT_EQ(bucket_size, table.Size());

    table.Flush();

    std::vector<std::vector<int> > keys(num_partitions, std::vector<int>());

    for (size_t i = 0; i != num_partitions; ++i) {
        auto it = files[i].GetReader();
        while (it.HasNext()) {
            auto n = it.Next<MyPair>();
            keys[i].push_back(n.second.key);
        }
    }

    for (size_t i = 0; i != num_partitions; ++i) {
        ASSERT_EQ(0u, table.PartitionSize(i));
    }
    ASSERT_EQ(num_partitions * num_buckets_init_scale, table.NumBuckets());
    ASSERT_EQ(0u, table.Size());

    // insert as many items which DO NOT lead to bucket overflow
    // (need to insert again because of previous flush call needed to backup data)
    for (size_t i = 0; i != bucket_size; ++i) {
        table.Insert(MyStruct(i, 0));
    }

    ASSERT_EQ(num_partitions * num_buckets_init_scale, table.NumBuckets());
    ASSERT_EQ(bucket_size, table.Size());

    // insert as many items guaranteed to DO lead to bucket overflow
    // resize happens here
    for (size_t i = 0; i != table.NumBuckets() * bucket_size; ++i) {
        table.Insert(MyStruct((int)(i + bucket_size), 1));
    }

    table.Flush();

    for (size_t i = 0; i != num_partitions; ++i) {
        ASSERT_EQ(0u, table.PartitionSize(i));
    }
    ASSERT_EQ(0u, table.Size());

    for (size_t i = 0; i != num_partitions; ++i) {
        auto it = files[i].GetReader();
        while (it.HasNext()) {
            auto n = it.Next<MyPair>();
            if (n.second.count == 0) {
                ASSERT_NE(keys[i].end(), std::find(keys[i].begin(), keys[i].end(), n.second.key));
            }
        }
    }
}

// Insert several items with same key and test application of local reduce
TEST_F(PreTable, InsertManyIntsAndTestReduce1) {
    auto key_ex = [](const MyStruct& in) {
                      return in.key % 500;
                  };

    auto red_fn = [](const MyStruct& in1, const MyStruct& in2) {
                      return MyStruct(in1.key, in1.count + in2.count);
                  };

    size_t total_sum = 0, total_count = 0;

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output);

    // Hashtable with smaller block size for testing.
    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn),
                              File::Writer, 16*1024>
    table(1, 2, 2, 128 * 1024, 1024 * 1024,
          key_ex, red_fn, writers);

    // insert lots of items
    size_t nitems = 1 * 1024 * 1024;
    for (size_t i = 0; i != nitems; ++i) {
        table.Insert(MyStruct(i, 1));
    }

    table.Flush();

    auto it1 = output.GetReader();
    while (it1.HasNext()) {
        auto n = it1.Next<MyPair>();
        total_count++;
        total_sum += n.second.count;
    }

    // actually check that the reduction worked
    ASSERT_EQ(500u, total_count);
    ASSERT_EQ(nitems, total_sum);
}

TEST_F(PreTable, InsertManyIntsAndTestReduce2) {
    auto key_ex = [](const MyStruct& in) {
                      return in.key;
                  };

    auto red_fn = [](const MyStruct& in1, const MyStruct& in2) {
                      return MyStruct(in1.key, in1.count + in2.count);
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output);

    size_t nitems_per_key = 10;
    size_t nitems = 1 * 32 * 1024;

    // Hashtable with smaller block size for testing.
    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn),
                              File::Writer, 16*1024>
    table(1, 2, 2, 128, nitems, key_ex, red_fn, writers);

    // insert lots of items
    int sum = 0;
    for (size_t i = 0; i != nitems_per_key; ++i) {
        sum += i;
        for (size_t j = 0; j != nitems; ++j) {
            table.Insert(MyStruct(j, i));
        }
    }

    ASSERT_EQ(nitems, table.Size());

    table.Flush();

    ASSERT_EQ(0u, table.Size());

    auto it1 = output.GetReader();
    while (it1.HasNext()) {
        auto n = it1.Next<MyPair>();
        ASSERT_EQ(sum, n.second.count);
    }
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
    auto key_ex = [](StringPair in) { return in.first; };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output);

    size_t nitems_per_key = 10;
    size_t nitems = 1 * 4 * 1024;

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn),
                              File::Writer, 16*1024>
    table(1, 2, 2, 128, nitems, key_ex, red_fn, writers);

    // insert lots of items
    int sum = 0;
    for (size_t j = 0; j != nitems; ++j) {
        sum = 0;
        std::string str;
        randomStr(str, 128);
        for (size_t i = 0; i != nitems_per_key; ++i) {
            sum += i;
            table.Insert(std::make_pair(str, i));
        }
    }

    ASSERT_EQ(nitems, table.Size());

    table.Flush();

    ASSERT_EQ(0u, table.Size());

    auto it1 = output.GetReader();
    while (it1.HasNext()) {
        auto n = it1.Next<StringPairPair>();
        ASSERT_EQ(sum, n.second.second);
    }
}

/******************************************************************************/
