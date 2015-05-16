/*******************************************************************************
 * tests/core/test_hash_table.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_pre_table_bench.hpp>
#include "gtest/gtest.h"
#include <tests/c7a_tests.hpp>
#include "c7a/api/context.hpp"

#include <stdio.h>
#include <functional>
#include <cstdio>

TEST(PreTableBench, CreateEmptyTable) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    assert(table.Size() == 0);
}


TEST(PreTableBench, AddIntegers) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    assert(table.Size() == 3);

    table.Insert(2);

    assert(table.Size() == 3);
}


TEST(PreTableBench, PopIntegers) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    table.SetMaxSize(3);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    assert(table.Size() == 0);

    table.Insert(1);

    assert(table.Size() == 1);
}

TEST(PreTableBench, FlushIntegers) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    assert(table.Size() == 3);

    table.Flush();

    assert(table.Size() == 0);

    table.Insert(1);

    assert(table.Size() == 1);
}


TEST(PreTableBench, ComplexType) {
    using StringPair = std::pair<std::string, double>;

    auto emit = [](StringPair in) {
                    std::cout << in.second << std::endl;
                };

    auto key_ex = [](StringPair in) { return in.first; };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    table.SetMaxSize(3);

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));
    table.Insert(std::make_pair("bonjour", 3));

    assert(table.Size() == 3);

    table.Insert(std::make_pair("hello", 5));

    assert(table.Size() == 3);

    table.Insert(std::make_pair("baguette", 42));

    assert(table.Size() == 0);
}



TEST(PreTableBench, MultipleWorkers) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) { return in; };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(2, key_ex, red_fn, { emit });

    assert(table.Size() == 0);
    table.SetMaxSize(5);

    for (int i = 0; i < 6; i++) {
        table.Insert(i * 35001);
    }

    assert(table.Size() <= 3);
    assert(table.Size() > 0);
}

// TODO(ms): add one test with a for loop inserting 10000 items. -> trigger
// resize!

/******************************************************************************/
