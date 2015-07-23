/*******************************************************************************
 * tests/core/pre_hash_table_probing_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
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

TEST_F(ReducePreProbingTable, CustomHashFunction) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    using HashTable = typename c7a::core::ReducePreProbingTable<
              decltype(key_ex), decltype(red_fn), File::Writer>;

    auto hash_function = [](int key, HashTable*) {

                             size_t global_index = key / 2;
                             size_t partition_id = 0;
                             size_t partition_offset = key / 2;

                             return HashTable::hash_result(partition_id, partition_offset, global_index);
                         };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    HashTable table(1, key_ex, red_fn, writers, std::make_pair(-1, -1), hash_function);

    for (int i = 0; i < 16; i++) {
        table.Insert(std::move(i));
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

TEST_F(ReducePreProbingTable, CustomProbingFunction) {
    auto key_ex = [](int in) {
        return in;
    };

    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    using HashTable = typename c7a::core::ReducePreProbingTable<
            decltype(key_ex), decltype(red_fn), File::Writer>;

    auto hash_function = [](int key, HashTable*) {

        size_t global_index = key / 2;
        size_t partition_id = 0;
        size_t partition_offset = key / 2;

        return HashTable::hash_result(partition_id, partition_offset, global_index);
    };

    auto probing_function = [](int post, HashTable*) {

        return HashTable::probing_result(1);
    };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    HashTable table(1, key_ex, red_fn, writers, std::make_pair(-1, -1), hash_function, probing_function);

    for (int i = 0; i < 16; i++) {
        table.Insert(std::move(i));
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, key_ex, red_fn, writers, std::make_pair(-1, -1));

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);

    ASSERT_EQ(3u, table.Size());

    table.Insert(2);

    ASSERT_EQ(3u, table.Size());
}

TEST_F(ReducePreProbingTable, CreateEmptyTable) {
    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, key_ex, red_fn, writers, std::make_pair(-1, -1));

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);

    ASSERT_EQ(3u, table.Size());

    table.Insert(0u);

    ASSERT_EQ(3u, table.Size());
}

TEST_F(ReducePreProbingTable, TestSetMaxSizeSetter) {
    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    auto key_ex = [](int in) { return in; };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, key_ex, red_fn, writers, std::make_pair(-1, -1));

    table.SetMaxSize(3);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(0u, table.Size());

    table.Insert(0);

    ASSERT_EQ(1u, table.Size());
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, key_ex, red_fn, writers, std::make_pair(-1, -1));

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5u, table.Size());

    table.Flush();

    auto it = output.GetReader();
    int c = 0;
    while (it.HasNext()) {
        it.Next<int>();
        c++;
    }

    ASSERT_EQ(5, c);
    ASSERT_EQ(0u, table.Size());
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(2, 5, 2, 10, 1.0f, 10, key_ex, red_fn, writers, std::make_pair(-1, -1));

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5u, table.Size());

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
    ASSERT_EQ(0u, table.Size());
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, 10, 2, 10, 1.0f, 4, key_ex, red_fn, writers, std::make_pair(-1, -1));

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(4u, table.Size());

    table.Insert(4);

    auto it = output.GetReader();
    int c = 0;
    while (it.HasNext()) {
        it.Next<int>();
        c++;
    }

    ASSERT_EQ(5, c);
    ASSERT_EQ(0u, table.Size());
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(2, 5, 2, 10, 1.0f, 4, key_ex, red_fn, writers, std::make_pair(-1, -1));

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
    ASSERT_EQ(0u, table.Size());
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, 10, 2, 10, 1.0f, 3, key_ex, red_fn, writers,
          std::pair<std::string, StringPair>("", std::pair<std::string, int>("", -1)));

    table.Insert(StringPair("hallo", 1));
    table.Insert(StringPair("hello", 1));
    table.Insert(StringPair("bonjour", 1));

    ASSERT_EQ(3u, table.Size());

    table.Insert(StringPair("hello", 1));

    ASSERT_EQ(3u, table.Size());

    table.Insert(StringPair("baguette", 1));

    ASSERT_EQ(0u, table.Size());
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(2, key_ex, red_fn, writers, std::make_pair(-1, -1));

    ASSERT_EQ(0u, table.Size());
    table.SetMaxSize(5);

    for (int i = 0; i < 6; i++) {
        table.Insert(i * 35001);
    }

    ASSERT_LE(table.Size(), 3u);
    ASSERT_GT(table.Size(), 0u);
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(1, 2, 10, 10, 1.0f, 10, key_ex, red_fn, writers, std::make_pair(-1, -1));

    table.Insert(0);

    ASSERT_EQ(2u, table.NumItems());
    ASSERT_EQ(1u, table.PartitionSize(0));
    ASSERT_EQ(1u, table.Size());

    table.Insert(1);

    ASSERT_EQ(2u, table.NumItems());
    ASSERT_EQ(2u, table.PartitionSize(0));
    ASSERT_EQ(2u, table.Size());

    table.Insert(2); // Resize happens here

    ASSERT_EQ(20u, table.NumItems());
    ASSERT_EQ(3u, table.PartitionSize(0));
    ASSERT_EQ(3u, table.Size());

    table.Flush();

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

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(2, 2, 10, 10, 1.0f, 10, key_ex, red_fn, writers, std::make_pair(-1, -1));

    ASSERT_EQ(0u, table.Size());
    ASSERT_EQ(4u, table.NumItems());
    ASSERT_EQ(0u, table.PartitionSize(0));
    ASSERT_EQ(0u, table.PartitionSize(1));

    table.Insert(0);
    table.Insert(1);

    ASSERT_EQ(2u, table.Size());
    ASSERT_EQ(4u, table.NumItems());
    ASSERT_EQ(1u, table.PartitionSize(0));
    ASSERT_EQ(1u, table.PartitionSize(1));

    table.Insert(2);

    ASSERT_EQ(3u, table.Size());
    ASSERT_EQ(4u, table.NumItems());
    ASSERT_EQ(2u, table.PartitionSize(0));
    ASSERT_EQ(1u, table.PartitionSize(1));

    table.Insert(3); // Resize happens here

    ASSERT_EQ(4u, table.Size());
    ASSERT_EQ(4u, table.NumItems());
    ASSERT_EQ(4u, table.PartitionSize(0) + table.PartitionSize(1));
}

TEST_F(ReducePreProbingTable, DISABLED_ResizeAndTestPartitionsHaveSameKeys) {
    auto key_ex = [](const IntPair in) {
                      return in.first;
                  };

    auto red_fn = [](const IntPair in1, const IntPair in2) {
                      return IntPair(in1.first, in1.second + in2.second);
                  };

    size_t num_partitions = 3;
    size_t num_items_init_scale = 2;
    size_t nitems = num_partitions * num_items_init_scale;

    std::vector<std::vector<int> > keys(num_partitions, std::vector<int>());

    std::vector<File> outputs;
    std::vector<File::Writer> writers;

    for (size_t i = 0; i != num_partitions; ++i) {
        File output;
        outputs.push_back(output);
        writers.push_back(output.GetWriter());
    }

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), File::Writer>
    table(num_partitions, num_items_init_scale, 10, 10, 1.0f,
          nitems, key_ex, red_fn, writers,
          std::pair<int, IntPair>(-1, std::pair<int, int>(-1, -1)));

    for (size_t i = 0; i != num_partitions; ++i) {
        ASSERT_EQ(0u, table.PartitionSize(i));
    }
    ASSERT_EQ(num_partitions * num_items_init_scale, table.NumItems());
    ASSERT_EQ(0u, table.Size());

    // insert as many items which DO NOT lead to partition overflow
    for (size_t i = 0; i != num_items_init_scale; ++i) {
        table.Insert(IntPair(i, 0));
    }

    ASSERT_EQ(num_partitions * num_items_init_scale, table.NumItems());
    ASSERT_EQ(num_items_init_scale, table.Size());

    table.Flush();

    for (size_t i = 0; i != num_partitions; ++i) {
        auto it = outputs[i].GetReader();
        while (it.HasNext()) {
            auto n = it.Next<IntPair>();
            keys[i].push_back(n.first);
        }
    }

    for (size_t i = 0; i != num_partitions; ++i) {
        ASSERT_EQ(0u, table.PartitionSize(i));
    }
    ASSERT_EQ(num_partitions * num_items_init_scale, table.NumItems());
    ASSERT_EQ(0u, table.Size());

    // insert as many items which DO NOT lead to partition overflow
    // (need to insert again because of previous flush call needed to backup data)
    for (size_t i = 0; i != num_items_init_scale; ++i) {
        table.Insert(IntPair(i, 0));
    }

    ASSERT_EQ(num_partitions * num_items_init_scale, table.NumItems());
    ASSERT_EQ(num_items_init_scale, table.Size());

    // insert as many items guaranteed to DO lead to partition overflow
    // resize happens here
    for (size_t i = 0; i != table.NumItems(); ++i) {
        table.Insert(IntPair(i, 1));
    }

    table.Flush();

    for (size_t i = 0; i != num_partitions; ++i) {
        ASSERT_EQ(0u, table.PartitionSize(i));
    }
    ASSERT_EQ(0u, table.Size());

    for (size_t i = 0; i != num_partitions; ++i) {
        auto it = outputs[i].GetReader();
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
    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn),
                                     File::Writer>
    table(1, 2, 2, nitems, 1.0f, nitems, key_ex, red_fn, writers,
          std::pair<int, IntPair>(-1, std::pair<int, int>(-1, -1)));

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
    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn),
                                     File::Writer>
    table(1, 2, 2, nitems, 1.0f, nitems, key_ex, red_fn, writers,
          std::pair<int, IntPair>(-1, std::pair<int, int>(-1, -1)));

    // insert lots of items
    int sum = 0;
    for (size_t i = 0; i != nitems_per_key; ++i) {
        sum += i;
        for (size_t j = 0; j != nitems; ++j) {
            table.Insert(IntPair(j, i));
        }
    }

    ASSERT_EQ(nitems, table.Size());

    table.Flush();

    ASSERT_EQ(0u, table.Size());

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

TEST_F(ReducePreProbingTable, DISABLED_InsertManyStringItemsAndTestReduce) {
    auto key_ex = [](StringPair in) { return in.first; };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    File output;
    std::vector<File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    size_t nitems_per_key = 2;
    size_t nitems = 1 * 4 * 1024;

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn),
                                     File::Writer>
    table(1, nitems, 2, nitems, 1.0f, nitems * 2, key_ex, red_fn, writers,
          std::pair<std::string, StringPair>("", std::pair<std::string, int>("", -1)));

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

    ASSERT_EQ(nitems, table.Size());

    table.Flush();

    ASSERT_EQ(0u, table.Size());

    auto it1 = output.GetReader();
    while (it1.HasNext()) {
        auto n = it1.Next<StringPair>();
        ASSERT_EQ(sum, n.second);
    }
}

/******************************************************************************/
