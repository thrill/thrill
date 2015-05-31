/*******************************************************************************
 * tests/core/post_hash_table_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_post_table.hpp>

#include "gtest/gtest.h"

using namespace c7a::data;
using namespace c7a::net;

struct PostTable : public::testing::Test {
    PostTable()
        : dispatcher(),
          multiplexer(dispatcher),
          manager(multiplexer),
          id(manager.AllocateDIA()),
          iterator(manager.GetIterator<int>(id)),
          pair_emit(manager.GetLocalEmitter<std::pair<std::string, int> >(id)) {
        emitters.emplace_back(manager.GetLocalEmitter<int>(id));
    }

    NetDispatcher                              dispatcher;
    ChannelMultiplexer                         multiplexer;
    DataManager                                manager;
    ChainId                                    id = manager.AllocateDIA();
    BlockIterator<int>                         iterator;
    BlockEmitter<std::pair<std::string, int> > pair_emit; //both emitters access the same dia id, which is bad if you use them both
    std::vector<BlockEmitter<int> >            emitters;

    size_t CountIteratorElements() {
        size_t result = 0;
        while (iterator.HasNext()) {
            result++;
            iterator.Next();
        }
        return result;
    }
};

TEST_F(PostTable, AddIntegers) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), BlockEmitter<int> >
    table(key_ex, red_fn, emitters);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    table.Print();

    ASSERT_EQ(3u, table.Size());

    table.Insert(2);

    table.Print();

    ASSERT_EQ(3u, table.Size());
}

TEST_F(PostTable, CreateEmptyTable) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), BlockEmitter<int> >
    table(key_ex, red_fn, emitters);

    ASSERT_EQ(0u, table.Size());
}

TEST_F(PostTable, FlusHIntegers) {
    auto key_ex = [](int in) {
                      return in;
                  };
    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), BlockEmitter<int> >
    table(key_ex, red_fn, emitters);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3u, table.Size());

    table.Flush();

    ASSERT_EQ(0u, table.Size());

    table.Insert(1);

    ASSERT_EQ(1u, table.Size());
}

TEST_F(PostTable, DISABLED_MultipleEmitters) { //TODO(ts) enable when hash table flushes emitters
    std::vector<int> vec1;

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    std::vector<BlockEmitter<int> > emitters;
    emitters.emplace_back(manager.GetLocalEmitter<int>(id));
    emitters.emplace_back(manager.GetLocalEmitter<int>(id));
    emitters.emplace_back(manager.GetLocalEmitter<int>(id));

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), BlockEmitter<int> >
    table(key_ex, red_fn, emitters);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3u, table.Size());

    table.Flush();

    ASSERT_EQ(0u, table.Size());

    table.Insert(1);

    ASSERT_EQ(1u, table.Size());

    ASSERT_EQ(9u, CountIteratorElements());
}

TEST_F(PostTable, ComplexType) {
    using StringPair = std::pair<std::string, int>;

    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    std::vector<BlockEmitter<StringPair> > emitters;
    emitters.emplace_back(manager.GetLocalEmitter<StringPair>(id));
    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), BlockEmitter<StringPair> >
    table(key_ex, red_fn, emitters);

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));
    table.Insert(std::make_pair("bonjour", 3));

    ASSERT_EQ(3u, table.Size());

    table.Insert(std::make_pair("hello", 5));

    ASSERT_EQ(3u, table.Size());

    table.Insert(std::make_pair("baguette", 42));

    ASSERT_EQ(4u, table.Size());
}

// TODO(ms): add one test with a for loop inserting 10000 items. -> trigger

/******************************************************************************/
