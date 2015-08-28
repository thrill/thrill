/*******************************************************************************
 * tests/core/pre_hash_table_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/core/reduce_pre_table.hpp>
#include <thrill/data/file.hpp>

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
    : public core::PreReduceByHashKey<int>
{
public:
    explicit CustomKeyHashFunction(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function)
    { }

    template <typename ReducePreTable>
    typename ReducePreTable::index_result
    operator () (const Key& k, ReducePreTable* ht) const {

        using index_result = typename ReducePreTable::index_result;

        size_t global_index = 0;
        size_t partition_id = 0;
        size_t local_index = 0;

        (void)k;
        (void)ht;

        return index_result(partition_id, local_index, global_index);
    }

private:
    HashFunction hash_function_;
};

TEST_F(PreTable, CustomHashFunction) {

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    data::BlockPool block_pool(nullptr);
    data::File output(block_pool);
    std::vector<data::File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    CustomKeyHashFunction<int> cust_hash;
    core::ReducePreTable<int, int, decltype(key_ex), decltype(red_fn), true,
                         CustomKeyHashFunction<int> >
    table(1, key_ex, red_fn, writers, 1024 * 16, 0.001, 1.0, cust_hash);

    for (int i = 0; i < 16; i++) {
        table.Insert(i);
    }

    table.Flush();

    auto it1 = output.GetReader();
    int c = 0;
    while (it1.HasNext()) {
        it1.Next<int>();
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

    data::BlockPool block_pool(nullptr);
    data::File output(block_pool);
    std::vector<data::File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    core::ReducePreTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3u, table.NumItems());

    table.Insert(2);

    ASSERT_EQ(3u, table.NumItems());
}

TEST_F(PreTable, CreateEmptyTable) {

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    data::BlockPool block_pool(nullptr);
    data::File output(block_pool);
    std::vector<data::File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    core::ReducePreTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3u, table.NumItems());

    table.Insert(2);

    ASSERT_EQ(3u, table.NumItems());
}

TEST_F(PreTable, DISABLED_PopIntegers) {

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    auto key_ex = [](int in) {
                      return in;
                  };

    data::BlockPool block_pool(nullptr);
    data::File output(block_pool);
    std::vector<data::File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    core::ReducePreTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers);

    //table.SetMaxNumItems(3);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(0u, table.NumItems());

    table.Insert(1);

    ASSERT_EQ(1u, table.NumItems());
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

    data::BlockPool block_pool(nullptr);
    data::File output(block_pool);
    std::vector<data::File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    core::ReducePreTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, 8 * 1024, 0.001, 1.0);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5u, table.NumItems());

    table.Flush();
    ASSERT_EQ(0u, table.NumItems());

    auto it = output.GetReader();
    int c = 0;
    while (it.HasNext()) {
        it.Next<int>();
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

    data::BlockPool block_pool(nullptr);
    data::File output1(block_pool), output2(block_pool);
    std::vector<data::File::Writer> writers;
    writers.emplace_back(output1.GetWriter());
    writers.emplace_back(output2.GetWriter());

    core::ReducePreTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(2, key_ex, red_fn, writers, 8 * 1024, 0.001, 1.0);

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5u, table.NumItems());

    table.Flush();
    ASSERT_EQ(0u, table.NumItems());

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
}

// Partial flush of items in table due to
// max table size constraint, one partition
TEST_F(PreTable, DISABLED_FlushIntegersPartiallyOnePartition) {

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    data::BlockPool block_pool(nullptr);
    data::File output(block_pool);
    std::vector<data::File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    core::ReducePreTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, 8 * 1024, 0.001, 1.0);

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

    data::BlockPool block_pool(nullptr);
    data::File output1(block_pool), output2(block_pool);

    std::vector<data::File::Writer> writers;
    writers.emplace_back(output1.GetWriter());
    writers.emplace_back(output2.GetWriter());

    core::ReducePreTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(2, key_ex, red_fn, writers, 8 * 1024, 0.001, 1.0);

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

TEST_F(PreTable, ComplexType) {

    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    data::BlockPool block_pool(nullptr);
    data::File output(block_pool);
    std::vector<data::File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    core::ReducePreTable<std::string, StringPair, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, 16 * 1024, 0.001, 0.5);

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));
    table.Insert(std::make_pair("bonjour", 3));

    ASSERT_EQ(3u, table.NumItems());

    table.Insert(std::make_pair("hello", 5));

    ASSERT_EQ(3u, table.NumItems());

    table.Insert(std::make_pair("baguette", 42));

    table.Flush();

    ASSERT_EQ(0u, table.NumItems());
}

TEST_F(PreTable, DISABLED_MultipleWorkers) {

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    data::BlockPool block_pool(nullptr);
    data::File output1(block_pool), output2(block_pool);

    std::vector<data::File::Writer> writers;
    writers.emplace_back(output1.GetWriter());
    writers.emplace_back(output2.GetWriter());

    core::ReducePreTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(2, key_ex, red_fn, writers, 1024 * 8, 0.001, 0.5);

    ASSERT_EQ(0u, table.NumItems());

    for (int i = 0; i < 6; i++) {
        table.Insert(i * 35001);
    }

    ASSERT_LE(table.NumItems(), 3u);
    ASSERT_GT(table.NumItems(), 0u);
}

// Insert several items with same key and test application of local reduce
TEST_F(PreTable, InsertManyIntsAndTestReduce1) {

    auto key_ex = [](const MyStruct& in) {
                      return in.key % 500;
                  };

    auto red_fn = [](const MyStruct& in1, const MyStruct& in2) {
                      return MyStruct {
                                 in1.key, in1.count + in2.count
                      };
                  };

    size_t total_sum = 0, total_count = 0;

    data::BlockPool block_pool(nullptr);
    data::File output(block_pool);
    std::vector<data::File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    size_t nitems = 1 * 1024 * 1024;

    // Hashtable with smaller block size for testing.
    core::ReducePreTable<size_t, MyStruct, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, nitems * 16, 0.001, 0.5);

    // insert lots of items
    for (size_t i = 0; i != nitems; ++i) {
        table.Insert(MyStruct { i, 1 });
    }

    table.Flush();

    auto it1 = output.GetReader();
    while (it1.HasNext()) {
        auto n = it1.Next<MyStruct>();
        total_count++;
        total_sum += n.count;
    }

    // actually check that the reduction worked
    ASSERT_EQ(500u, total_count);
    ASSERT_EQ(nitems, total_sum);
}

TEST_F(PreTable, DISABLED_InsertManyIntsAndTestReduce2) {

    auto key_ex = [](const MyStruct& in) {
                      return in.key;
                  };

    auto red_fn = [](const MyStruct& in1, const MyStruct& in2) {
                      return MyStruct {
                                 in1.key, in1.count + in2.count
                      };
                  };

    data::BlockPool block_pool(nullptr);
    data::File output(block_pool);
    std::vector<data::File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    size_t nitems_per_key = 10;
    size_t nitems = 1 * 32 * 1024;

    // Hashtable with smaller block size for testing.
    core::ReducePreTable<size_t, MyStruct, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, nitems * 16, 0.001, 0.5);

    // insert lots of items
    int sum = 0;
    for (size_t i = 0; i != nitems_per_key; ++i) {
        sum += i;
        for (size_t j = 0; j != nitems; ++j) {
            table.Insert(MyStruct { j, static_cast<int>(i) });
        }
    }

    table.Flush();

    ASSERT_EQ(0u, table.NumItems());

    auto it1 = output.GetReader();
    while (it1.HasNext()) {
        auto n = it1.Next<MyStruct>();
        ASSERT_EQ(sum, n.count);
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

    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    data::BlockPool block_pool(nullptr);
    data::File output(block_pool);
    std::vector<data::File::Writer> writers;
    writers.emplace_back(output.GetWriter());

    size_t nitems_per_key = 10;
    size_t nitems = 1 * 4 * 1024;

    core::ReducePreTable<std::string, StringPair, decltype(key_ex), decltype(red_fn), true>
    table(1, key_ex, red_fn, writers, 16 * 1024, 0.001, 0.5);

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

    table.Flush();

    ASSERT_EQ(0u, table.NumItems());

    auto it1 = output.GetReader();
    while (it1.HasNext()) {
        auto n = it1.Next<StringPair>();
        ASSERT_EQ(sum, n.second);
    }
}

/******************************************************************************/
