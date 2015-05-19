/*******************************************************************************
 * tests/core/test_hash_table.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_pre_table.hpp>
#include <c7a/core/reduce_post_table.hpp>
#include <tests/c7a_tests.hpp>
#include "c7a/api/context.hpp"
#include <c7a/data/data_manager.hpp>
#include <c7a/data/block_emitter.hpp>
#include <c7a/net/channel_multiplexer.hpp>

#include <functional>
#include <cstdio>
#include <utility>
#include <vector>
#include <string>

#include "gtest/gtest.h"

struct PostTable : public::testing::Test {
    PostTable()
        : dispatcher(),
          multiplexer(dispatcher),
          manager(multiplexer),
          id(manager.AllocateDIA()),
          emit(manager.GetLocalEmitter<int>(id)),
          iterator(manager.GetLocalBlocks<int>(id)),
          pair_emit(manager.GetLocalEmitter<std::pair<std::string, int> >(id)) { }

    c7a::net::NetDispatcher                               dispatcher;
    c7a::net::ChannelMultiplexer                          multiplexer;
    c7a::data::DataManager                                manager;
    size_t                                                id = manager.AllocateDIA();
    c7a::data::BlockEmitter<int>                          emit;
    c7a::data::BlockIterator<int>                         iterator;
    c7a::data::BlockEmitter<std::pair<std::string, int> > pair_emit; //both emitters access the same dia id, which is bad if you use them both

    size_t CountIteratorElements() {
        size_t result = 0;
        while(iterator.HasNext()) {
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

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(key_ex, red_fn, { emit });

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    table.Print();

    ASSERT_EQ(3, table.Size());

    table.Insert(2);

    table.Print();

    ASSERT_EQ(3, table.Size());
}

TEST_F(PostTable, CreateEmptyTable) {

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(key_ex, red_fn, { emit });

    ASSERT_EQ(0, table.Size());
}

TEST_F(PostTable, FlusHIntegers) {
    auto key_ex = [](int in) {
                      return in;
                  };
    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(key_ex, red_fn, { emit });

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3, table.Size());

    table.Flush();

    ASSERT_EQ(0, table.Size());

    table.Insert(1);

    ASSERT_EQ(1, table.Size());
}

TEST_F(PostTable, MultipleEmitters) {
    std::vector<int> vec1;

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    std::vector<decltype(emit)> emitters;
    emitters.push_back(emit);
    emitters.push_back(emit);
    emitters.push_back(emit);

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), decltype(emit1)>
    table(key_ex, red_fn, emitters);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3, table.Size());

    table.Flush();

    ASSERT_EQ(0, table.Size());

    table.Insert(1);

    ASSERT_EQ(1, table.Size());

    ASSERT_EQ(9, CountIteratorElements());
}

TEST_F(PostTable, ComplexType) {
    using StringPair = std::pair<std::string, double>;

    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), decltype(pair_emit)>
    table(key_ex, red_fn, { pair_emit });

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));
    table.Insert(std::make_pair("bonjour", 3));

    ASSERT_EQ(3, table.Size());

    table.Insert(std::make_pair("hello", 5));

    ASSERT_EQ(3, table.Size());

    table.Insert(std::make_pair("baguette", 42));

    ASSERT_EQ(4, table.Size());
}


// TODO(ms): add one test with a for loop inserting 10000 items. -> trigger

/******************************************************************************/
