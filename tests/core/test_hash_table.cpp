/*******************************************************************************
 * tests/core/test_hash_table.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/core/hash_table.hpp>
#include "gtest/gtest.h"
#include <tests/c7a_tests.hpp>
#include "c7a/api/context.hpp"
#include <c7a/data/data_manager.hpp>

#include <functional>
#include <cstdio>

TEST(HashTable, CreateEmptyTable) {
    c7a::data::DataManager manager;
    auto id = manager.AllocateDIA();
    auto emit = manager.GetLocalEmitter<int>(id);

    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::HashTable<decltype(key_ex), decltype(red_fn)>
    table(1, key_ex, red_fn, emit);

    assert(table.Size() == 0);
}

TEST(HashTable, AddIntegers) {
    c7a::data::DataManager manager;
    auto id = manager.AllocateDIA();
    auto emit = manager.GetLocalEmitter<int>(id);

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

TEST(HashTable, PopIntegers) {
    c7a::data::DataManager manager;
    auto id = manager.AllocateDIA();
    auto emit = manager.GetLocalEmitter<int>(id);

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

TEST(HashTable, FlushIntegers) {
    c7a::data::DataManager manager;
    auto id = manager.AllocateDIA();
    auto emit = manager.GetLocalEmitter<int>(id);

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

TEST(HashTable, ComplexType) {
    using StringPair = std::pair<std::string, int>;

    c7a::data::DataManager manager;
    auto id = manager.AllocateDIA();
    auto emit = manager.GetLocalEmitter<std::pair<std::string, int> >(id);

    auto key_ex = [](StringPair in) { return in.first; };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    c7a::core::HashTable<decltype(key_ex), decltype(red_fn)>
    table(1, key_ex, red_fn, emit);

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
