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
    size_t                                                id = manager.AllocateDIA();
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

TEST_F(PreTable, FlushIntegers) {
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

    table.Flush();

    ASSERT_EQ(0, table.Size());

    table.Insert(1);

    ASSERT_EQ(1, table.Size());
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

TEST_F(PreTable, Resize) {
    using StringPair = std::pair<std::string, int>;

    auto key_ex = [](StringPair in) { return in.first; };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    std::vector<c7a::data::BlockEmitter<StringPair> > emitters;
    emitters.push_back(pair_emit);
    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(pair_emit)>
    table(1, 10, 2, 1, 10, key_ex, red_fn, emitters);

    ASSERT_EQ(10, table.NumBuckets());

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));

    ASSERT_EQ(10, table.NumBuckets());

    table.Print();

    table.Insert(std::make_pair("bonjour", 3));

    table.Print();

    //ASSERT_EQ(20, table.NumBuckets()); // TODO(ms): fix (strange, passes locally)
}

/******************************************************************************/
