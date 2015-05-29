/*******************************************************************************
 * tests/core/pre_hash_table_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_pre_table.hpp>

#include "gtest/gtest.h"

struct PreTable : public::testing::Test {
    PreTable()
        : dispatcher(),
          multiplexer(dispatcher),
          manager(multiplexer),
          id(manager.AllocateDIA()),
          emit(manager.GetLocalEmitter<int>(id)),
          pair_emit(manager.GetLocalEmitter<std::pair<std::string, int> >(id)),
          int_pair_emit(manager.GetLocalEmitter<std::pair<int, int> >(id))
    { }

    c7a::net::NetDispatcher                               dispatcher;
    c7a::net::ChannelMultiplexer                          multiplexer;
    c7a::data::DataManager                                manager;
    c7a::data::DIAId                                      id = manager.AllocateDIA();
    c7a::data::BlockEmitter<int>                          emit;
    // all emitters access the same dia id, which is bad if you use them both
    c7a::data::BlockEmitter<std::pair<std::string, int> > pair_emit;
    c7a::data::BlockEmitter<std::pair<int, int> >         int_pair_emit;
};

TEST_F(PreTable, AddIntegers) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3, table.Size());

    table.Insert(2);

    ASSERT_EQ(3, table.Size());
}

TEST_F(PreTable, CreateEmptyTable) {
    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3, table.Size());

    table.Insert(2);

    ASSERT_EQ(3, table.Size());
}

TEST_F(PreTable, PopIntegers) {
    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    auto key_ex = [](int in) { return in; };

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    table.SetMaxSize(3);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(0, table.Size());

    table.Insert(1);

    ASSERT_EQ(1, table.Size());
}

// Manually flush all items in table,
// no size constraint, one partition
TEST_F(PreTable, DISABLED_FlushIntegersManuallyOnePartition) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    auto id1 = manager.AllocateDIA();
    auto emit1 = manager.GetLocalEmitter<int>(id1);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit1)>
    table(1, 10, 2, 10, 10, key_ex, red_fn, { emit1 });

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5, table.Size());

    table.Flush();
    emit1.Flush();

    auto it = manager.GetIterator<int>(id1);
    int c = 0;
    while (it.HasNext()) {
        std::cout << "test" << std::endl;
        it.Next();
        c++;
    }

    ASSERT_EQ(5, c);
    ASSERT_EQ(0, table.Size());
}

// Manually flush all items in table,
// no size constraint, two partitions
TEST_F(PreTable, DISABLED_FlushIntegersManuallyTwoPartitions) {
    auto key_ex = [](int in) {
        return in;
    };

    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    auto id1 = manager.AllocateDIA();
    auto emit1 = manager.GetLocalEmitter<int>(id1);
    auto id2 = manager.AllocateDIA();
    auto emit2 = manager.GetLocalEmitter<int>(id2);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit1)>
            table(2, 5, 2, 10, 10, key_ex, red_fn, { emit1, emit2 });

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(5, table.Size());

    table.Flush();
    emit1.Flush();
    emit2.Flush();

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
    ASSERT_EQ(0, table.Size());
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

    auto id1 = manager.AllocateDIA();
    auto emit1 = manager.GetLocalEmitter<int>(id1);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit1)>
            table(1, 10, 2, 10, 4, key_ex, red_fn, { emit1 });

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(4, table.Size());

    table.Insert(4);

    emit1.Flush();

    auto it = manager.GetIterator<int>(id1);
    int c = 0;
    while (it.HasNext()) {
        it.Next();
        c++;
    }

    ASSERT_EQ(5, c);
    ASSERT_EQ(0, table.Size());
}

//// Partial flush of items in table due to
//// max table size constraint, two partitions
TEST_F(PreTable, DISABLED_FlushIntegersPartiallyTwoPartitions) {
    auto key_ex = [](int in) {
        return in;
    };

    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    auto id1 = manager.AllocateDIA();
    auto emit1 = manager.GetLocalEmitter<int>(id1);
    auto id2 = manager.AllocateDIA();
    auto emit2 = manager.GetLocalEmitter<int>(id2);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit1)>
            table(2, 5, 2, 10, 4, key_ex, red_fn, { emit1, emit2 });

    table.Insert(0);
    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(4, table.Size());

    table.Insert(4);

    emit1.Flush();
    emit2.Flush();

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

    ASSERT_EQ(0, c2);
    ASSERT_EQ(2, table.Size());
}

TEST_F(PreTable, ComplexType) {
    using StringPair = std::pair<std::string, int>;

    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(pair_emit)>
    table(1, 2, 2, 10, 3, key_ex, red_fn, { pair_emit });

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));
    table.Insert(std::make_pair("bonjour", 3));

    ASSERT_EQ(3, table.Size());

    table.Insert(std::make_pair("hello", 5));

    ASSERT_EQ(3, table.Size());

    table.Insert(std::make_pair("baguette", 42));

    ASSERT_EQ(0, table.Size());
}

TEST_F(PreTable, BigTest) {

    struct MyStruct
    {
        int key;
        int count;

        // only initializing constructor, no default construction possible.
        explicit MyStruct(int k, int c) : key(k), count(c)
        { }
    };

    auto key_ex = [](const MyStruct& in) {
                      return in.key % 500;
                  };

    auto red_fn = [](const MyStruct& in1, const MyStruct& in2) {
        return MyStruct(in1.key, in1.count + in2.count);
    };

    size_t total_sum = 0, total_count = 0;

    auto emit_fn = [&](const MyStruct& in) {
        total_count++;
        total_sum += in.count;
    };

    // Hashtable with smaller block size for testing.
    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn),
                              decltype(emit_fn), 16* 1024>
    table(1, 2, 2, 128 * 1024, 1024 * 1024,
          key_ex, red_fn, { emit_fn });

    // insert lots of items
    size_t nitems = 1 * 1024 * 1024;
    for (size_t i = 0; i != nitems; ++i) {
        table.Insert(MyStruct(i, 1));
    }

    table.Flush();

    // actually check that the reduction worked
    ASSERT_EQ(total_count, 500);
    ASSERT_EQ(total_sum, nitems);
}

TEST_F(PreTable, MultipleWorkers) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(2, key_ex, red_fn, { emit });

    ASSERT_EQ(0, table.Size());
    table.SetMaxSize(5);

    for (int i = 0; i < 6; i++) {
        table.Insert(i * 35001);
    }

    ASSERT_LE(table.Size(), 3);
    ASSERT_GT(table.Size(), 0);
}

// Resize due to max bucket size reached. Set max items per bucket to 1,
// then add 2 items with different key, but having same hash value, one partition
TEST_F(PreTable, DISABLED_ResizeOnePartition) {
    using StringPair = std::pair<std::string, int>;

    auto key_ex = [](StringPair in) { return in.first; };

    auto red_fn = [](StringPair in1, StringPair in2) {
        return std::make_pair(in1.first, in1.second + in2.second);
    };

    auto id1 = manager.AllocateDIA();
    auto emit1 = manager.GetLocalEmitter<StringPair>(id1);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit1)>
            table(1, 10, 2, 1, 10, key_ex, red_fn, { emit1 });

    ASSERT_EQ(10, table.NumBuckets());

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));

    ASSERT_EQ(2, table.Size());
    ASSERT_EQ(10, table.NumBuckets());
    ASSERT_EQ(2, table.PartitionSize(0));

    table.Insert(std::make_pair("bonjour", 3)); // Resize happens here

    ASSERT_EQ(3, table.Size());
    ASSERT_EQ(20, table.NumBuckets()); // TODO(ms): fix (strange, passes locally)
    ASSERT_EQ(3, table.PartitionSize(0));

    table.Flush();
    emit1.Flush();

    auto it1 = manager.GetIterator<StringPair>(id1);
    int c = 0;
    while (it1.HasNext()) {
        it1.Next();
        c++;
    }

    ASSERT_EQ(3, c);
}

// Resize due to max bucket size reached. Set max items per bucket to 1,
// then add 2 items with different key, but having same hash value, two partitions
// Check that same items are in same partition after resize
TEST_F(PreTable, DISABLED_ResizeTwoPartitions) {
    using StringPair = std::pair<std::string, int>;

    auto key_ex = [](StringPair in) { return in.first; };

    auto red_fn = [](StringPair in1, StringPair in2) {
        return std::make_pair(in1.first, in1.second + in2.second);
    };

    auto id1 = manager.AllocateDIA();
    auto emit1 = manager.GetLocalEmitter<StringPair>(id1);
    auto id2 = manager.AllocateDIA();
    auto emit2 = manager.GetLocalEmitter<StringPair>(id2);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit1)>
            table(2, 10, 2, 1, 10, key_ex, red_fn, {emit1, emit2});

    ASSERT_EQ(20, table.NumBuckets());

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));

    ASSERT_EQ(2, table.Size());
    ASSERT_EQ(20, table.NumBuckets());
    ASSERT_EQ(2, table.PartitionSize(0));
    ASSERT_EQ(0, table.PartitionSize(1));

    table.Insert(std::make_pair("bonjour", 3)); // Resize happens here

    ASSERT_EQ(3, table.Size());
    ASSERT_EQ(40, table.NumBuckets()); // TODO(ms): fix (strange, passes locally)
    ASSERT_EQ(3, table.PartitionSize(0));
    ASSERT_EQ(0, table.PartitionSize(1));

    table.Flush();
    emit1.Flush();
    emit2.Flush();

    auto it1 = manager.GetIterator<StringPair>(id1);
    int c1 = 0;
    while (it1.HasNext()) {
        it1.Next();
        c1++;
    }
    ASSERT_EQ(3, c1);

    auto it2 = manager.GetIterator<StringPair>(id2);
    int c2 = 0;
    while (it2.HasNext()) {
        it2.Next();
        c2++;
    }
    ASSERT_EQ(0, c2);
}

// Insert several items with same key and test application of local reduce
TEST_F(PreTable, DISABLED_Reduce) {
    using StringPair = std::pair<std::string, int>;

    auto key_ex = [](StringPair in) { return in.first; };

    auto red_fn = [](StringPair in1, StringPair in2) {
        return std::make_pair(in1.first, in1.second + in2.second);
    };

    auto id1 = manager.AllocateDIA();
    auto emit1 = manager.GetLocalEmitter<StringPair>(id1);

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit1)>
            table(1, 10, 2, 10, 10, key_ex, red_fn, {emit1});

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 22));
    table.Insert(std::make_pair("bonjour", 3));

    ASSERT_EQ(3, table.Size());

    table.Insert(std::make_pair("hallo", 2));
    table.Insert(std::make_pair("hello", 33));
    table.Insert(std::make_pair("bonjour", 44));

    ASSERT_EQ(3, table.Size());

    table.Flush();
    emit1.Flush();

    auto it1 = manager.GetIterator<StringPair>(id1);
    int c1 = 0;
    while (it1.HasNext()) {
        StringPair p = it1.Next();
        if (p.first == "hallo") {
            ASSERT_EQ(3, p.second);
            c1++;
        } else if (p.first == "hello") {
            ASSERT_EQ(55, p.second);
            c1++;
        } else if (p.first == "bonjour") {
            ASSERT_EQ(47, p.second);
            c1++;
        }
    }
    ASSERT_EQ(3, c1);
}

/******************************************************************************/
