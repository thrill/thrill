/*******************************************************************************
 * tests/api/test_simple_api.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include <tests/c7a-tests.hpp>
#include "c7a/api/dia.hpp"
#include "c7a/api/context.hpp"
#include "c7a/api/function_stack.hpp"
#include "c7a/engine/stage_builder.hpp"

using namespace c7a::engine;

TEST(DIASimple, SharedPtrTest) {
    using c7a::DIA;
    using c7a::Context;

    Context ctx;

    auto map_fn = [](int in) {
                      return 2 * in;
                  };
    auto key_ex = [](int in) {
                      return in % 2;
                  };
    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    auto input = ReadFromFileSystem(
        ctx,
        g_workpath + "/inputs/test1",
        [](const std::string& line) {
            return std::stoi(line);
        });
    DIA<int> ints = input.Map(map_fn);
    // DIA<int> doubles = ints.Map(map_fn);
    auto doubles = ints.Map(map_fn);
    // Do this to keep reference count alive;
    DIA<int> test = ints;
    ints = doubles;
    // auto quad = doubles.Map(map_fn);
    auto red_quad = doubles.ReduceBy(key_ex).With(red_fn);

    std::cout << "Input: " << input.NodeString() << " RefCount: " << input.get_node_count() << std::endl;
    std::cout << "Ints: " << ints.NodeString() << " RefCount: " << ints.get_node_count() << std::endl;
    std::cout << "Doubles: " << doubles.NodeString() << " RefCount: " << doubles.get_node_count() << std::endl;
    // std::cout << "Quad: " << quad.NodeString() << " RefCount: " << quad.get_node_count() << std::endl;
    std::cout << "Red: " << red_quad.NodeString() << " RefCount: " << red_quad.get_node_count() << std::endl;
    std::vector<Stage> result;
    FindStages(red_quad.get_node(), result);
    for (auto s : result)
    {
        s.Run();
    }

    return;
}

TEST(DIASimple, FunctionStackTest) {
    using c7a::FunctionStack;
    std::vector<double> elements;

    // User-defined functions
    auto fmap_fn =
        [ = ](double input, std::function<void(double)> emit_func) {
            emit_func(input);
            emit_func(input);
        };

    auto map_fn =
        [ = ](double input) {
            return 2 * input;
        };

    auto filter_fn =
        [ = ](double input) {
            return input > 80;
        };

    auto save_fn =
        [&elements](double input) {
            elements.push_back(input);
        };

    // Converted emitter functions
    auto conv_map_fn =
        [ = ](double input, std::function<void(double)> emit_func) {
            emit_func(map_fn(input));
        };

    auto conv_filter_fn =
        [ = ](double input, std::function<void(double)> emit_func) {
            if (filter_fn(input)) emit_func(input);
        };

    std::cout << "==============" << std::endl;
    std::cout << "FunctionStack" << std::endl;
    std::cout << "==============" << std::endl;
    FunctionStack<> stack;
    auto new_stack = stack.push(fmap_fn);
    auto new_stack2 = new_stack.push(conv_map_fn);
    auto new_stack3 = new_stack2.push(conv_filter_fn);
    auto new_stack4 = new_stack3.push(save_fn);
    auto composed_function = new_stack4.emit();
    composed_function(42);
    composed_function(2);
    composed_function(50);

    for (auto item : elements) {
        std::cout << item << std::endl;
    }
    return;
}

/******************************************************************************/
