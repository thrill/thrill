/*******************************************************************************
 * tests/core/pre_hash_table_probing_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/core/reduce_pre_probing_table.hpp>

#include <gtest/gtest.h>
#include <thrill/data/file.hpp>

#include <string>
#include <utility>
#include <vector>

using namespace thrill;

using StringPair = std::pair<std::string, int>;
using IntPair = std::pair<int, int>;

struct ReducePreProbingTable : public::testing::Test {
    ReducePreProbingTable() : block_pool(nullptr, nullptr, "pre-probing-table"), output(block_pool) { }

    data::BlockPool block_pool;
    data::File      output;
};

template <typename Key, typename HashFunction = std::hash<Key> >
class CustomKeyHashFunction
    : public core::PreProbingReduceByHashKey<int>
{
public:
    explicit CustomKeyHashFunction(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function)
    { }

    template <typename ReducePreProbingTable>
    typename ReducePreProbingTable::index_result
    operator () (const Key& v, ReducePreProbingTable* ht) const {

        using index_result = typename ReducePreProbingTable::index_result;

        size_t global_index = v / 2;
        size_t partition_id = 0;
        size_t local_index = v / 2;

        (void)ht;

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

    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output.GetDynWriter());

    CustomKeyHashFunction<int> cust_hash;
    core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true,
                                CustomKeyHashFunction<int> >
    table(1, key_ex, red_fn, writers, -1, 1024 * 16, 0.5, cust_hash);

    for (int i = 0; i < 16; i++) {
        table.Insert(i);
    }

    table.Flush();

    auto it = output.GetKeepReader();
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

    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output.GetDynWriter());

    core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 1024 * 16, 1.0);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);

    ASSERT_EQ(3u, table.NumItems());

    table.Insert(2);

    ASSERT_EQ(3u, table.NumItems());
}

TEST_F(ReducePreProbingTable, CreateEmptyTable) {

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output.GetDynWriter());

    core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 1024 * 16, 1.0);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);

    ASSERT_EQ(3u, table.NumItems());

    table.Insert(0u);

    ASSERT_EQ(3u, table.NumItems());
}

TEST_F(ReducePreProbingTable, DISABLED_TestSetMaxSizeSetter) {

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    auto key_ex = [](int in) {
                      return in;
                  };

    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output.GetDynWriter());

    core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 1024 * 16, 1.0);

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

    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output.GetDynWriter());

    core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 1024 * 16, 1.0);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5u, table.NumItems());

    table.Flush();

    auto it = output.GetKeepReader();
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

    data::File output1(block_pool), output2(block_pool);
    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output1.GetDynWriter());
    writers.emplace_back(output2.GetDynWriter());

    core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(2, key_ex, red_fn, writers, -1, 1024 * 16, 1.0);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5u, table.NumItems());

    table.Flush();

    auto it1 = output1.GetKeepReader();
    int c1 = 0;
    while (it1.HasNext()) {
        it1.Next<int>();
        c1++;
    }

    ASSERT_EQ(3, c1);

    auto it2 = output2.GetKeepReader();
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

    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output.GetDynWriter());

    core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, 2 * 4 * 2 * 4, 0.5);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(4u, table.NumItems());

    table.Insert(4);

    auto it = output.GetKeepReader();
    int c = 0;
    while (it.HasNext()) {
        it.Next<int>();
        c++;
    }

    ASSERT_EQ(4, c);
    ASSERT_EQ(1u, table.NumItems());
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

    data::File output1(block_pool), output2(block_pool);
    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output1.GetDynWriter());
    writers.emplace_back(output2.GetDynWriter());

    core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(2, key_ex, red_fn, writers, -1, 1024 * 16, 1.0);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(4u, table.NumItems());

    table.Insert(4);
    table.Flush();

    auto it1 = output1.GetKeepReader();
    int c1 = 0;
    while (it1.HasNext()) {
        it1.Next<int>();
        c1++;
    }

    ASSERT_EQ(3, c1);
    table.Flush();

    auto it2 = output2.GetKeepReader();
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

    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output.GetDynWriter());

    size_t kv_size = sizeof(core::ReducePreProbingTable<std::string, StringPair,
                                                        decltype(key_ex), decltype(red_fn), true>::KeyValuePair);

    core::ReducePreProbingTable<std::string, StringPair, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, "", 2 * 3 * kv_size, 0.5);

    table.Insert(StringPair("hallo", 1));
    table.Insert(StringPair("hello", 1));
    table.Insert(StringPair("bonjour", 1));

    ASSERT_EQ(3u, table.NumItems());

    table.Insert(StringPair("hello", 1));

    ASSERT_EQ(3u, table.NumItems());

    table.Insert(StringPair("baguette", 1));

    ASSERT_EQ(1u, table.NumItems());
}

TEST_F(ReducePreProbingTable, MultipleWorkers) {

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    data::File output1(block_pool), output2(block_pool);
    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output1.GetDynWriter());
    writers.emplace_back(output2.GetDynWriter());

    core::ReducePreProbingTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(2, key_ex, red_fn, writers, -1, 6 * 8, 0.5);

    ASSERT_EQ(0u, table.NumItems());

    for (int i = 0; i < 6; i++) {
        table.Insert(i * 35001);
    }

    ASSERT_LE(table.NumItems(), 3u);
    ASSERT_GT(table.NumItems(), 0u);
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

    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output.GetDynWriter());

    size_t nitems = 1 * 1024 * 1024;

    // Hashtable with smaller block size for testing.
    core::ReducePreProbingTable<int, IntPair, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, nitems * 16, 1.0);

    // insert lots of items
    for (size_t i = 0; i != nitems; ++i) {
        table.Insert(IntPair(i, 1));
    }

    table.Flush();

    auto it1 = output.GetKeepReader();
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

    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output.GetDynWriter());

    size_t nitems_per_key = 10;
    size_t nitems = 1 * 32 * 1024;

    // Hashtable with smaller block size for testing.
    core::ReducePreProbingTable<int, IntPair, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, -1, nitems * 16, 1.0);

    // insert lots of items
    size_t sum = 0;
    for (size_t i = 0; i != nitems_per_key; ++i) {
        sum += i;
        for (size_t j = 0; j != nitems; ++j) {
            table.Insert(IntPair(j, i));
        }
    }

    ASSERT_EQ(nitems, table.NumItems());

    table.Flush();

    ASSERT_EQ(0u, table.NumItems());

    auto it1 = output.GetKeepReader();
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

    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    std::vector<data::File::DynWriter> writers;
    writers.emplace_back(output.GetDynWriter());

    size_t nitems_per_key = 2;
    size_t nitems = 1 * 4 * 1024;

    size_t kv_size = sizeof(core::ReducePreProbingTable<std::string, StringPair,
                                                        decltype(key_ex), decltype(red_fn), true>::KeyValuePair);

    core::ReducePreProbingTable<std::string, StringPair, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, "", nitems * kv_size, 1.0);

    // insert lots of items
    size_t sum = 0;
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

    auto it1 = output.GetKeepReader();
    while (it1.HasNext()) {
        auto n = it1.Next<StringPair>();
        ASSERT_EQ(sum, n.second);
    }
}

/******************************************************************************/
