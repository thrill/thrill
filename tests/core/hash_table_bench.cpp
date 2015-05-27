/*******************************************************************************
 * tests/core/hash_table_bench.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_pre_table.hpp>
#include <c7a/core/reduce_pre_table_bench.hpp>
#include <tests/c7a_tests.hpp>
#include <c7a/api/context.hpp>

#include <functional>
#include <cstdio>

#include "gtest/gtest.h"

struct BenchTable : public::testing::Test {
    std::function<void(int)> emit   = [](int /*in*/){ };
    std::function<int(int)>  key_ex = [](int in){ return in; };
    std::function<int(int, int)> red_fn = [](int in1, int in2){ return in1 + in2; };
};

TEST_F(BenchTable, ActualTable1KKInts) {
    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    for (int i = 0; i < 1000000; i++) {
        table.Insert(i * 17);
    }

    table.Flush();
}

TEST_F(BenchTable, DISABLED_ChausTable1KKInts) {//I(ts) disabled this test because it does the same as the one above
    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    for (int i = 0; i < 1000000; i++) {
        table.Insert(i * 17);
    }

    table.Flush();
}

TEST_F(BenchTable, ActualTable10Workers) {
    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(10, key_ex, red_fn, { emit, emit, emit, emit, emit, emit, emit, emit, emit, emit });

    for (int i = 0; i < 1000000; i++) {
        table.Insert(i * 17);
    }

    table.Flush();
}

TEST_F(BenchTable, DISABLED_ChausTable10Workers) {//I(ts) disabled this test because it does the same as the one above

    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(10, key_ex, red_fn, { emit });

    for (int i = 0; i < 1000000; i++) {
        table.Insert(i * 17);
    }

    table.Flush();
}

// TODO(ms): add one test with a for loop inserting 10000 items. -> trigger
// resize!

/******************************************************************************/
