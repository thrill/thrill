/*******************************************************************************
 * tests/core/test_hash_table.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_pre_table.hpp>
#include <c7a/core/reduce_pre_table_bench.hpp>
#include "gtest/gtest.h"
#include <tests/c7a_tests.hpp>
#include "c7a/api/context.hpp"

#include <stdio.h>
#include <functional>
#include <cstdio>

TEST(BenchTable, ActualTable) {
    auto emit = [] (int in) {
        //std::cout << in << std::endl;
    };

    auto key_ex = [] (int in) {
        return in;
    };

    auto red_fn = [] (int in1, int in2) {
        return in1 + in2;
    };

    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    for (int i = 0; i < 1000000; i++) {
        table.Insert(i * 17);
    }
    
    table.Flush();
}

TEST(BenchTable, ChausTable) {
    auto emit = [] (int in) {
        //std::cout << in << std::endl;
    };

    auto key_ex = [] (int in) {
        return in;
    };

    auto red_fn = [] (int in1, int in2) {
        return in1 + in2;
    };

    c7a::core::ReducePreTableBench<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

     for (int i = 0; i < 1000000; i++) {
        table.Insert(i * 17);
    }
    
     table.Flush();
}


// TODO(ms): add one test with a for loop inserting 10000 items. -> trigger
// resize!

/******************************************************************************/
