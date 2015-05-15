/*******************************************************************************
 * tests/core/test_stage_builder.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef DEBUG
#define DEBUG = FALSE
#endif

#include "gtest/gtest.h"
#include <tests/c7a_tests.hpp>
#include "c7a/api/dia_base.hpp"
#include "c7a/core/stage_builder.hpp"
#include "c7a/api/dia.hpp"
#include "c7a/api/context.hpp"

using namespace c7a::core;

TEST(Stage, GetStagesFromBuilder) {
    using c7a::DIARef;
    using c7a::Context;
    Context ctx;
    auto doubles = ReadFromFileSystem(ctx,
                                      g_workpath + "/inputs/test1",
                                      [] (std::string line) {
                                          return std::stod(line);
                                      });

    auto key = [] (double in) {
        return (int)in;
    };
    auto red_fn = [] (double in1, double in2) {
        return in1 + in2;
    }
    auto map_fn = [] (double input) {
        std::cout << "Map" << std::endl;
        return input;
    };

    /*auto fmap_fn = [](double input, std::function<void(double)> emit_func) {
                       std::cout << "FlatMap" << std::endl;
                       emit_func(input);
                       emit_func(input);
                       };*/

    auto duplicates = doubles.Map(map_fn);
    // auto duplicates2 = duplicates.Map(map_fn);
    auto doubles2 = doubles.ReduceBy(key).With(red_fn);

    // auto duplicates3 = red_duplicates.Map(map_fn);
    auto red_duplicates2 = doubles2.ReduceBy(key).With(red_fn);

    //SIMULATE

    RunScope(red_duplicates2.get_node());
}

/******************************************************************************/
