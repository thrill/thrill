/*******************************************************************************
 * tests/core/pre_hash_table_probing_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_pre_probing_table.hpp>

#include <c7a/data/file.hpp>
#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

using namespace c7a::data;

using StringPair = std::pair<std::string, int>;
using IntPair = std::pair<int, int>;

struct ReducePreProbingTable : public::testing::Test { };

template <typename Key, typename HashFunction = std::hash<Key> >
class CustomKeyHashFunction
        : public c7a::core::PreProbingReduceByHashKey<int> {
public:
    CustomKeyHashFunction(const HashFunction& hash_function = HashFunction())
            : hash_function_(hash_function)
    { }

    template <typename ReducePreProbingTable>
    typename ReducePreProbingTable::index_result
    operator () (Key v, ReducePreProbingTable* ht) const {

        using index_result = typename ReducePreProbingTable::index_result;

        size_t global_index = v / 2;
        size_t partition_id = 0;
        size_t local_index = v / 2;

        (*ht).NumItems();

        return index_result(partition_id, local_index, global_index);
    }

private:
    HashFunction hash_function_;
};

TEST_F(ReducePreProbingTable, CustomHashFunction) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    CustomKeyHashFunction<int> cust_hash;
    c7a::core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true,
            CustomKeyHashFunction<int>>
    table(1, key_ex, red_fn, writers, -1, 10, 2, 1.0f, 1048576, cust_hash);

    for (int i = 0; i < 16; i++) {
        table.Insert(i);
    }

    table.Flush();

    auto it = output.GetReader();
    int c = 0;
    while (it.HasNext()) {
        it.Next<int>();
        c++;
    }

    ASSERT_EQ(16, c);
}

TEST_F(ReducePreProbingTable, AddIntegers) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    c7a::core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 10, 2, 1.0f, 1048576);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);

    ASSERT_EQ(3u, table.NumItems());

    table.Insert(2);

    ASSERT_EQ(3u, table.NumItems());
}

TEST_F(ReducePreProbingTable, CreateEmptyTable) {
    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    c7a::core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 10, 2, 1.0f, 1048576);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);

    ASSERT_EQ(3u, table.NumItems());

    table.Insert(0u);

    ASSERT_EQ(3u, table.NumItems());
}

TEST_F(ReducePreProbingTable, TestSetMaxSizeSetter) {
    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    auto key_ex = [](int in) { return in; };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    c7a::core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 10, 2, 1.0f, 1048576);

    table.SetMaxNumItems(3);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(0u, table.NumItems());

    table.Insert(0);

    ASSERT_EQ(1u, table.NumItems());
}

// Manually flush all items in table,
// no size constraint, one partition
TEST_F(ReducePreProbingTable, FlushIntegersManuallyOnePartition) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    c7a::core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 10, 2, 1.0f, 1048576);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5u, table.NumItems());

    table.Flush();

    auto it = output.GetReader();
    int c = 0;
    while (it.HasNext()) {
        it.Next<int>();
        c++;
    }

    ASSERT_EQ(5, c);
    ASSERT_EQ(0u, table.NumItems());
}

// Manually flush all items in table,
// no size constraint, two partitions
TEST_F(ReducePreProbingTable, FlushIntegersManuallyTwoPartitions) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output1, output2;
    std::vector<File::Writer> writers;
    writers.emplace_back(output1.GetWriter());
    writers.emplace_back(output2.GetWriter());

    c7a::core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(2, key_ex, red_fn, writers, -1, 5, 2, 1.0f, 10);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5u, table.NumItems());

    table.Flush();

    auto it1 = output1.GetReader();
    int c1 = 0;
    while (it1.HasNext()) {
        it1.Next<int>();
        c1++;
    }

    ASSERT_EQ(3, c1);

    auto it2 = output2.GetReader();
    int c2 = 0;
    while (it2.HasNext()) {
        it2.Next<int>();
        c2++;
    }

    ASSERT_EQ(2, c2);
    ASSERT_EQ(0u, table.NumItems());
}

// Partial flush of items in table due to
// max table size constraint, one partition
TEST_F(ReducePreProbingTable, FlushIntegersPartiallyOnePartition) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    c7a::core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 10, 2, 1.0f, 4);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(4u, table.NumItems());

    table.Insert(4);

    auto it = output.GetReader();
    int c = 0;
    while (it.HasNext()) {
        it.Next<int>();
        c++;
    }

    ASSERT_EQ(5, c);
    ASSERT_EQ(0u, table.NumItems());
}

//// Partial flush of items in table due to
//// max table size constraint, two partitions
TEST_F(ReducePreProbingTable, FlushIntegersPartiallyTwoPartitions) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output1, output2;
    std::vector<File::Writer> writers;
    writers.emplace_back(output1.GetWriter());
    writers.emplace_back(output2.GetWriter());

    c7a::core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(2, key_ex, red_fn, writers, -1, 5, 2, 1.0f, 4);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(4u, table.NumItems());

    table.Insert(4);
    table.Flush();

    auto it1 = output1.GetReader();
    int c1 = 0;
    while (it1.HasNext()) {
        it1.Next<int>();
        c1++;
    }

    ASSERT_EQ(3, c1);
    table.Flush();

    auto it2 = output2.GetReader();
    int c2 = 0;
    while (it2.HasNext()) {
        it2.Next<int>();
        c2++;
    }

    ASSERT_EQ(2, c2);
    ASSERT_EQ(0u, table.NumItems());
}

TEST_F(ReducePreProbingTable, ComplexType) {
    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return StringPair(in1.first, in1.second + in2.second);
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    c7a::core::ReducePreProbingTable<std::string, StringPair, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, "", 10, 2, 1.0f, 3);

    table.Insert(StringPair("hallo", 1));
    table.Insert(StringPair("hello", 1));
    table.Insert(StringPair("bonjour", 1));

    ASSERT_EQ(3u, table.NumItems());

    table.Insert(StringPair("hello", 1));

    ASSERT_EQ(3u, table.NumItems());

    table.Insert(StringPair("baguette", 1));

    ASSERT_EQ(0u, table.NumItems());
}

TEST_F(ReducePreProbingTable, MultipleWorkers) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output1, output2;
    std::vector<File::Writer> writers;
    writers.emplace_back(output1.GetWriter());
    writers.emplace_back(output2.GetWriter());

    c7a::core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(2, key_ex, red_fn, writers, -1, 10, 2, 1.0f, 1048576);

    ASSERT_EQ(0u, table.NumItems());
    table.SetMaxNumItems(5);

    for (int i = 0; i < 6; i++) {
        table.Insert(i * 35001);
    }

    ASSERT_LE(table.NumItems(), 3u);
    ASSERT_GT(table.NumItems(), 0u);
}

// Resize due to max partition fill ratio reached. Set max partition fill ratio to 1.0f,
// then add 2 items with different key, but having same hash value, one partition
TEST_F(ReducePreProbingTable, ResizeOnePartition) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    c7a::core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 2, 10, 1.0f, 10);

    table.Insert(0);

    ASSERT_EQ(2u, table.Size());
    ASSERT_EQ(1u, table.PartitionNumItems(0));
    ASSERT_EQ(1u, table.NumItems());

    table.Insert(1);

    ASSERT_EQ(2u, table.Size());
    ASSERT_EQ(2u, table.PartitionNumItems(0));
    ASSERT_EQ(2u, table.NumItems());

    table.Insert(2); // Resize happens here

    ASSERT_EQ(20u, table.Size());
    ASSERT_EQ(3u, table.PartitionNumItems(0));
    ASSERT_EQ(3u, table.NumItems());

    table.Flush();
    table.CloseEmitter();

    auto it1 = output.GetReader();
    int c = 0;
    while (it1.HasNext()) {
        it1.Next<int>();
        c++;
    }

    ASSERT_EQ(3, c);
}

// Resize due to max partition fill ratio reached. Set max partition fill ratio to 1.0f,
// then add 2 items with different key, but having same hash value, two partitions
// Check that same items are in same partition after resize
TEST_F(ReducePreProbingTable, ResizeTwoPartitions) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output1, output2;
    std::vector<File::Writer> writers;
    writers.emplace_back(output1.GetWriter());
    writers.emplace_back(output2.GetWriter());

    c7a::core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(2, key_ex, red_fn, writers, -1, 2, 10, 1.0f, 10);

    ASSERT_EQ(0u, table.NumItems());
    ASSERT_EQ(4u, table.Size());
    ASSERT_EQ(0u, table.PartitionNumItems(0));
    ASSERT_EQ(0u, table.PartitionNumItems(1));

    table.Insert(0);
    table.Insert(1);

    ASSERT_EQ(2u, table.NumItems());
    ASSERT_EQ(4u, table.Size());
    ASSERT_EQ(1u, table.PartitionNumItems(0));
    ASSERT_EQ(1u, table.PartitionNumItems(1));

    table.Insert(2);

    ASSERT_EQ(3u, table.NumItems());
    ASSERT_EQ(4u, table.Size());
    ASSERT_EQ(2u, table.PartitionNumItems(0));
    ASSERT_EQ(1u, table.PartitionNumItems(1));

    table.Insert(3); // Resize happens here

    ASSERT_EQ(4u, table.NumItems());
    ASSERT_EQ(4u, table.Size());
    ASSERT_EQ(4u, table.PartitionNumItems(0) + table.PartitionNumItems(1));
}

TEST_F(ReducePreProbingTable, ResizeAndTestPartitionsHaveSameKeysAfterResize) {
    auto key_ex = [](const IntPair in) {
                      return in.first;
                  };

    auto red_fn = [](const IntPair in1, const IntPair in2) {
                      return IntPair(in1.first, in1.second + in2.second);
                  };

    size_t num_partitions = 100;
    size_t num_items_init_scale = 1024 * 2;
    size_t nitems = num_partitions * num_items_init_scale;

    std::vector<std::vector<int> > keys(num_partitions, std::vector<int>());

    std::vector<File> files(num_partitions);
    std::vector<File::Writer> writers;
    for (size_t i = 0; i != num_partitions; ++i) {
        writers.emplace_back(files[i].GetWriter());
    }

    c7a::core::ReducePreProbingTable<int, IntPair, decltype(key_ex), decltype(red_fn), true>
    table(num_partitions, key_ex, red_fn, writers, -1, num_items_init_scale,
          10, 1.0f,
          nitems);

    for (size_t i = 0; i != num_partitions; ++i) {
        ASSERT_EQ(0u, table.PartitionNumItems(i));
    }
    ASSERT_EQ(num_partitions * num_items_init_scale, table.Size());
    ASSERT_EQ(0u, table.NumItems());

    // insert as many items which DO NOT lead to partition overflow
    for (size_t i = 0; i != num_items_init_scale; ++i) {
        table.Insert(IntPair(i, 0));
    }

    ASSERT_EQ(num_partitions * num_items_init_scale, table.Size());
    ASSERT_EQ(num_items_init_scale, table.NumItems());

    table.Flush();

    for (size_t i = 0; i != num_partitions; ++i) {
        auto it = files[i].GetReader();
        while (it.HasNext()) {
            auto n = it.Next<IntPair>();
            keys[i].push_back(n.first);
        }
    }

    for (size_t i = 0; i != num_partitions; ++i) {
        ASSERT_EQ(0u, table.PartitionNumItems(i));
    }
    ASSERT_EQ(num_partitions * num_items_init_scale, table.Size());
    ASSERT_EQ(0u, table.NumItems());

    // insert as many items which DO NOT lead to partition overflow
    // (need to insert again because of previous flush call needed to backup data)
    for (size_t i = 0; i != num_items_init_scale; ++i) {
        table.Insert(IntPair(i, 0));
    }

    ASSERT_EQ(num_partitions * num_items_init_scale, table.Size());
    ASSERT_EQ(num_items_init_scale, table.NumItems());

    // insert as many items guaranteed to DO lead to partition overflow
    // resize happens here
    for (size_t i = 0; i != table.Size(); ++i) {
        table.Insert(IntPair(i, 1));
    }

    table.Flush();

    for (size_t i = 0; i != num_partitions; ++i) {
        ASSERT_EQ(0u, table.PartitionNumItems(i));
    }
    ASSERT_EQ(0u, table.NumItems());

    for (size_t i = 0; i != num_partitions; ++i) {
        auto it = files[i].GetReader();
        while (it.HasNext()) {
            auto n = it.Next<IntPair>();
            if (n.second == 0) {
                ASSERT_NE(keys[i].end(), std::find(keys[i].begin(), keys[i].end(), n.first));
            }
        }
    }
}

// Insert several items with same key and test application of local reduce
TEST_F(ReducePreProbingTable, InsertManyIntsAndTestReduce1) {
    auto key_ex = [](const IntPair in) {
                      return in.first % 500;
                  };

    auto red_fn = [](const IntPair in1, const IntPair in2) {
                      return IntPair(in1.first, in1.second + in2.second);
                  };

    size_t total_sum = 0, total_count = 0;

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    size_t nitems = 1 * 1024 * 1024;

    // Hashtable with smaller block size for testing.
    c7a::core::ReducePreProbingTable<int, IntPair, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 2, 2, 1.0f, nitems);

    // insert lots of items
    for (size_t i = 0; i != nitems; ++i) {
        table.Insert(IntPair(i, 1));
    }

    table.Flush();

    auto it1 = output.GetReader();
    while (it1.HasNext()) {
        auto n = it1.Next<IntPair>();
        total_count++;
        total_sum += n.second;
    }

    // actually check that the reduction worked
    ASSERT_EQ(500u, total_count);
    ASSERT_EQ(nitems, total_sum);
}

TEST_F(ReducePreProbingTable, InsertManyIntsAndTestReduce2) {
    auto key_ex = [](const IntPair in) {
                      return in.first;
                  };

    auto red_fn = [](const IntPair in1, const IntPair in2) {
                      return IntPair(in1.first, in1.second + in2.second);
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    size_t nitems_per_key = 10;
    size_t nitems = 1 * 32 * 1024;

    // Hashtable with smaller block size for testing.
    c7a::core::ReducePreProbingTable<int, IntPair, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 2, 2, 1.0f, nitems);

    // insert lots of items
    int sum = 0;
    for (size_t i = 0; i != nitems_per_key; ++i) {
        sum += i;
        for (size_t j = 0; j != nitems; ++j) {
            table.Insert(IntPair(j, i));
        }
    }

    ASSERT_EQ(nitems, table.NumItems());

    table.Flush();

    ASSERT_EQ(0u, table.NumItems());

    auto it1 = output.GetReader();
    while (it1.HasNext()) {
        auto n = it1.Next<IntPair>();
        ASSERT_EQ(sum, n.second);
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

TEST_F(ReducePreProbingTable, InsertManyStringItemsAndTestReduce) {
    auto key_ex = [](StringPair in) { return in.first; };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    size_t nitems_per_key = 2;
    size_t nitems = 1 * 4 * 1024;

    c7a::core::ReducePreProbingTable<std::string, StringPair, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, "", nitems, 2, 1.0f, nitems * 2);

    // insert lots of items
    int sum = 0;
    for (size_t j = 0; j != nitems; ++j) {
        sum = 0;
        std::string str;
        randomStr(str, 10);
        for (size_t i = 0; i != nitems_per_key; ++i) {
            sum += i;
            table.Insert(StringPair(str, i));
        }
    }

    ASSERT_EQ(nitems, table.NumItems());

    table.Flush();

    ASSERT_EQ(0u, table.NumItems());

    auto it1 = output.GetReader();
    while (it1.HasNext()) {
        auto n = it1.Next<StringPair>();
        ASSERT_EQ(sum, n.second);
    }
}

/******************************************************************************/
