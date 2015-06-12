/*******************************************************************************
 * tests/core/pre_hash_table_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_pre_linpro_table.hpp>

#include "gtest/gtest.h"

using namespace c7a::data;
using namespace c7a::net;
using StringPair = std::pair<std::string, int>;

struct ReducePreLinProTable : public::testing::Test {
    ReducePreLinProTable()
        : dispatcher(),
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

TEST_F(ReducePreLinProTable, AddIntegers) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreLinProTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(1, key_ex, red_fn, one_int_emitter);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3u, table.Size());

    table.Insert(2);

    ASSERT_EQ(3u, table.Size());
}

TEST_F(ReducePreLinProTable, CreateEmptyTable) {
    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    c7a::core::ReducePreLinProTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
            table(1, key_ex, red_fn, one_int_emitter);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3u, table.Size());

    table.Insert(2);

    ASSERT_EQ(3u, table.Size());
}

TEST_F(ReducePreLinProTable, PopIntegers) {
    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    auto key_ex = [](int in) { return in; };

    c7a::core::ReducePreLinProTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
            table(1, key_ex, red_fn, one_int_emitter);

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
TEST_F(ReducePreLinProTable, FlushIntegersManuallyOnePartition) {
    auto key_ex = [](int in) {
        return in;
    };

    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    c7a::core::ReducePreLinProTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
            table(1, 10, 2, 1, 10, 1.0f, 10, key_ex, red_fn, one_int_emitter);

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
TEST_F(ReducePreLinProTable, FlushIntegersManuallyTwoPartitions) {
    auto key_ex = [](int in) {
        return in;
    };

    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    c7a::core::ReducePreLinProTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
            table(2, 5, 2, 1, 10, 1.0f, 10, key_ex, red_fn, two_int_emitters);

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
TEST_F(ReducePreLinProTable, FlushIntegersPartiallyOnePartition) {
    auto key_ex = [](int in) {
        return in;
    };

    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    c7a::core::ReducePreLinProTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
            table(1, 10, 2, 1, 10, 1.0f, 4, key_ex, red_fn, one_int_emitter);

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
TEST_F(ReducePreLinProTable, FlushIntegersPartiallyTwoPartitions) {
    auto key_ex = [](int in) {
        return in;
    };

    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    c7a::core::ReducePreLinProTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
            table(2, 5, 2, 1, 10, 1.0f, 4, key_ex, red_fn, two_int_emitters);

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

/******************************************************************************/
