/*******************************************************************************
 * tests/core/pre_hash_table_probing_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_pre_probing_table.hpp>

#include "gtest/gtest.h"

using namespace c7a::data;
using namespace c7a::net;
using StringPair = std::pair<std::string, int>;
using IntIntPair = std::pair<int, int>;

struct ReducePreProbingTable : public::testing::Test {
    ReducePreProbingTable()
        : dispatcher("dispatcher"),
          manager(dispatcher),
          id1(manager.AllocateDIA()),
          id2(manager.AllocateDIA()) {
        one_int_emitter.emplace_back(manager.GetLocalEmitter<int>(id1));
        one_pair_emitter.emplace_back(manager.GetLocalEmitter<StringPair>(id1));

        two_int_emitters.emplace_back(manager.GetLocalEmitter<int>(id1));
        two_int_emitters.emplace_back(manager.GetLocalEmitter<int>(id2));

        two_pair_emitters.emplace_back(manager.GetLocalEmitter<StringPair>(id1));
        two_pair_emitters.emplace_back(manager.GetLocalEmitter<StringPair>(id2));
    }

    DispatcherThread                  dispatcher;
    Manager                           manager;
    DIAId                             id1;
    DIAId                             id2;
    // all emitters access the same dia id, which is bad if you use them both
    std::vector<Emitter<int> >        one_int_emitter;
    std::vector<Emitter<int> >        two_int_emitters;
    std::vector<Emitter<StringPair> > one_pair_emitter;
    std::vector<Emitter<StringPair> > two_pair_emitters;
};

TEST_F(ReducePreProbingTable, AddIntegers) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(1, key_ex, red_fn, one_int_emitter, std::make_pair(-1, -1));

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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(1, key_ex, red_fn, one_int_emitter, std::make_pair(-1, -1));

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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(1, key_ex, red_fn, one_int_emitter, std::make_pair(-1, -1));

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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(1, 10, 2, 1, 10, 1.0f, 10, key_ex, red_fn, one_int_emitter, std::make_pair(-1, -1));

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5u, table.Size());

    table.Flush();

    auto it = manager.GetIterator<int>(id1);
    int c = 0;
    while (it.HasNext()) {
        it.Next();
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(2, 5, 2, 1, 10, 1.0f, 10, key_ex, red_fn, two_int_emitters, std::make_pair(-1, -1));

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5u, table.Size());

    table.Flush();

    auto it1 = manager.GetIterator<int>(id1);
    int c1 = 0;
    while (it1.HasNext()) {
        it1.Next();
        c1++;
    }

    ASSERT_EQ(3, c1);

    auto it2 = manager.GetIterator<int>(id2);
    int c2 = 0;
    while (it2.HasNext()) {
        it2.Next();
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(1, 10, 2, 1, 10, 1.0f, 4, key_ex, red_fn, one_int_emitter, std::make_pair(-1, -1));

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(4u, table.Size());

    table.Insert(4);

    auto it = manager.GetIterator<int>(id1);
    int c = 0;
    while (it.HasNext()) {
        it.Next();
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(2, 5, 2, 1, 10, 1.0f, 4, key_ex, red_fn, two_int_emitters, std::make_pair(-1, -1));

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(4u, table.Size());

    table.Insert(4);
    table.Flush();

    auto it1 = manager.GetIterator<int>(id1);
    int c1 = 0;
    while (it1.HasNext()) {
        it1.Next();
        c1++;
    }

    ASSERT_EQ(3, c1);
    table.Flush();

    auto it2 = manager.GetIterator<int>(id2);
    int c2 = 0;
    while (it2.HasNext()) {
        it2.Next();
        c2++;
    }

    ASSERT_EQ(2, c2);
    ASSERT_EQ(0u, table.Size());
}

TEST_F(ReducePreProbingTable, DISABLED_ComplexType) {
    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return StringPair(in1.first, in1.second + in2.second);
                  };

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), Emitter<StringPair> >
    table(1, 10, 2, 1, 10, 1.0f, 3, key_ex, red_fn, one_pair_emitter,
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(2, key_ex, red_fn, one_int_emitter, std::make_pair(-1, -1));

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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(1, 2, 10, 1, 10, 1.0f, 10, key_ex, red_fn, one_int_emitter, std::make_pair(-1, -1));

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

    auto it1 = manager.GetIterator<int>(id1);
    int c = 0;
    while (it1.HasNext()) {
        it1.Next();
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

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(2, 2, 10, 1, 10, 1.0f, 10, key_ex, red_fn, two_int_emitters, std::make_pair(-1, -1));

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
    ASSERT_EQ(40u, table.NumItems());
    ASSERT_EQ(4u, table.PartitionSize(0) + table.PartitionSize(1));
}

TEST_F(ReducePreProbingTable, ResizeAndTestPartitionsHaveSameKeys) {
    auto key_ex = [](const IntIntPair in) {
                      return in.first;
                  };

    auto red_fn = [](const IntIntPair in1, const IntIntPair in2) {
                      return IntIntPair(in1.first, in1.second + in2.second);
                  };

    size_t num_partitions = 3;
    size_t num_items_init_scale = 2;
    size_t nitems = num_partitions * num_items_init_scale;

    std::vector<Emitter<IntIntPair> > emitters;
    std::vector<std::vector<int> > keys(num_partitions, std::vector<int>());
    std::vector<DIAId> ids;
    for (size_t i = 0; i != num_partitions; ++i) {
        auto id = manager.AllocateDIA();
        ids.emplace_back(id);
        emitters.emplace_back(manager.GetLocalEmitter<IntIntPair>(id));
    }

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), Emitter<IntIntPair> >
    table(num_partitions, num_items_init_scale, 10, 1, 10, 1.0f,
          nitems, key_ex, red_fn, { emitters },
          std::pair<int, IntIntPair>(-1, std::pair<int, int>(-1, -1)));

    for (size_t i = 0; i != num_partitions; ++i) {
        ASSERT_EQ(0u, table.PartitionSize(i));
    }
    ASSERT_EQ(num_partitions * num_items_init_scale, table.NumItems());
    ASSERT_EQ(0u, table.Size());

    // insert as many items which DO NOT lead to partition overflow
    for (size_t i = 0; i != num_items_init_scale; ++i) {
        table.Insert(IntIntPair(i, 0));
    }

    ASSERT_EQ(num_partitions * num_items_init_scale, table.NumItems());
    ASSERT_EQ(num_items_init_scale, table.Size());

    table.Flush();

    for (size_t i = 0; i != num_partitions; ++i) {
        auto it = manager.GetIterator<IntIntPair>(ids[i]);
        while (it.HasNext()) {
            auto n = it.Next();
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
        table.Insert(IntIntPair(i, 0));
    }

    ASSERT_EQ(num_partitions * num_items_init_scale, table.NumItems());
    ASSERT_EQ(num_items_init_scale, table.Size());

    // insert as many items guaranteed to DO lead to partition overflow
    // resize happens here
    for (size_t i = 0; i != table.NumItems(); ++i) {
        table.Insert(IntIntPair(i, 1));
    }

    table.Flush();

    for (size_t i = 0; i != num_partitions; ++i) {
        ASSERT_EQ(0u, table.PartitionSize(i));
    }
    ASSERT_EQ(0u, table.Size());

    for (size_t i = 0; i != num_partitions; ++i) {
        auto it = manager.GetIterator<IntIntPair>(ids[i]);
        while (it.HasNext()) {
            auto n = it.Next();
            if (n.second == 0) {
                ASSERT_NE(keys[i].end(), std::find(keys[i].begin(), keys[i].end(), n.first));
            }
        }
    }
}

// Insert several items with same key and test application of local reduce
TEST_F(ReducePreProbingTable, InsertManyIntsAndTestReduce1) {
    auto key_ex = [](const IntIntPair in) {
                      return in.first % 500;
                  };

    auto red_fn = [](const IntIntPair in1, const IntIntPair in2) {
                      return IntIntPair(in1.first, in1.second + in2.second);
                  };

    size_t total_sum = 0, total_count = 0;

    auto id1 = manager.AllocateDIA();
    std::vector<Emitter<IntIntPair> > emitters;
    emitters.emplace_back(manager.GetLocalEmitter<IntIntPair>(id1));

    size_t nitems = 1 * 1024 * 1024;

    // Hashtable with smaller block size for testing.
    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn),
                                     Emitter<IntIntPair> >
    table(1, 2, 2, 1, nitems, 2.0f, nitems, key_ex, red_fn, { emitters },
          std::pair<int, IntIntPair>(-1, std::pair<int, int>(-1, -1)));

    // insert lots of items
    for (size_t i = 0; i != nitems; ++i) {
        table.Insert(IntIntPair(i, 1));
    }

    table.Flush();

    auto it1 = manager.GetIterator<IntIntPair>(id1);
    while (it1.HasNext()) {
        auto n = it1.Next();
        total_count++;
        total_sum += n.second;
    }

    // actually check that the reduction worked
    ASSERT_EQ(500u, total_count);
    ASSERT_EQ(nitems, total_sum);
}

TEST_F(ReducePreProbingTable, InsertManyIntsAndTestReduce2) {
    auto key_ex = [](const IntIntPair in) {
                      return in.first;
                  };

    auto red_fn = [](const IntIntPair in1, const IntIntPair in2) {
                      return IntIntPair(in1.first, in1.second + in2.second);
                  };

    auto id1 = manager.AllocateDIA();
    std::vector<Emitter<IntIntPair> > emitters;
    emitters.emplace_back(manager.GetLocalEmitter<IntIntPair>(id1));

    size_t nitems_per_key = 10;
    size_t nitems = 1 * 32 * 1024;

    // Hashtable with smaller block size for testing.
    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn),
                                     Emitter<IntIntPair> >
    table(1, 2, 2, 1, nitems, 1.0f, nitems, key_ex, red_fn, { emitters },
          std::pair<int, IntIntPair>(-1, std::pair<int, int>(-1, -1)));

    // insert lots of items
    int sum = 0;
    for (size_t i = 0; i != nitems_per_key; ++i) {
        sum += i;
        for (size_t j = 0; j != nitems; ++j) {
            table.Insert(IntIntPair(j, i));
        }
    }

    ASSERT_EQ(nitems, table.Size());

    table.Flush();

    ASSERT_EQ(0u, table.Size());

    auto it1 = manager.GetIterator<IntIntPair>(id1);
    while (it1.HasNext()) {
        auto n = it1.Next();
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

    auto id1 = manager.AllocateDIA();
    std::vector<Emitter<StringPair> > emitters;
    emitters.emplace_back(manager.GetLocalEmitter<StringPair>(id1));

    size_t nitems_per_key = 2;
    size_t nitems = 1 * 4 * 1024;

    c7a::core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn),
                                     Emitter<StringPair> >
    table(1, nitems, 2, 1, nitems, 1.0f, nitems, key_ex, red_fn, { emitters },
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

    auto it1 = manager.GetIterator<StringPair>(id1);
    while (it1.HasNext()) {
        auto n = it1.Next();
        ASSERT_EQ(sum, n.second);
    }
}

/******************************************************************************/
