/*******************************************************************************
 * tests/core/test_hash_table.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/core/hash_table.hpp>
#include "gtest/gtest.h"
#include <tests/c7a_tests.hpp>
#include "c7a/api/context.hpp"
#include <c7a/data/data_manager.hpp>
#include <c7a/data/block_emitter.hpp>
#include <c7a/net/channel_multiplexer.hpp>

#include <functional>
#include <cstdio>

struct HashTableTest : public::testing::Test {
    HashTableTest()
        : dispatcher(),
          multiplexer(dispatcher, 1),
          manager(multiplexer),
          id(manager.AllocateDIA()),
          emit(manager.GetLocalEmitter<int>(id)),
          pair_emit(manager.GetLocalEmitter<std::pair<std::string, int> >(id)) { }

    c7a::net::NetDispatcher                               dispatcher;
    c7a::net::ChannelMultiplexer                          multiplexer;
    c7a::data::DataManager                                manager;
    size_t                                                id = manager.AllocateDIA();
    c7a::data::BlockEmitter<int>                          emit;
    c7a::data::BlockEmitter<std::pair<std::string, int> > pair_emit; //both emitters access the same dia id, which is bad if you use them both
};

TEST_F(HashTableTest, CreateEmptyTable) {
    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::HashTable<decltype(key_ex), decltype(red_fn)>
    table(1, key_ex, red_fn, emit);

    assert(table.Size() == 0);
}

TEST_F(HashTableTest, AddIntegers) {
    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::HashTable<decltype(key_ex), decltype(red_fn)>
    table(1, key_ex, red_fn, emit);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    assert(table.Size() == 3);

    table.Insert(2);

    assert(table.Size() == 3);
}

TEST_F(HashTableTest, PopIntegers) {
    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::HashTable<decltype(key_ex), decltype(red_fn)>
    table(1, key_ex, red_fn, emit);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    assert(table.Size() == 0);

    table.Insert(1);

    assert(table.Size() == 1);
}

TEST_F(HashTableTest, FlushIntegers) {
    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::HashTable<decltype(key_ex), decltype(red_fn)>
    table(1, key_ex, red_fn, emit);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    assert(table.Size() == 3);

    table.Flush();

    assert(table.Size() == 0);

    table.Insert(1);

    assert(table.Size() == 1);
}

TEST_F(HashTableTest, ComplexType) {
    using StringPair = std::pair<std::string, int>;

    auto key_ex = [](StringPair in) { return in.first; };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    c7a::core::HashTable<decltype(key_ex), decltype(red_fn)>
    table(1, key_ex, red_fn, pair_emit);

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));
    table.Insert(std::make_pair("bonjour", 3));

    assert(table.Size() == 3);

    table.Insert(std::make_pair("hello", 5));

    assert(table.Size() == 3);

    table.Insert(std::make_pair("baguette", 42));

    assert(table.Size() == 0);
}

// TODO(ms): add one test with a for loop inserting 10000 items. -> trigger
// resize!

/******************************************************************************/
