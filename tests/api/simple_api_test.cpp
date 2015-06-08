/*******************************************************************************
 * tests/api/simple_api_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/dia.hpp>
#include <c7a/api/context.hpp>
#include <c7a/api/function_stack.hpp>

#include <string>
#include <vector>

#include <gtest/gtest.h>

using namespace c7a::core;

TEST(API, FunctionStackTest) {
    using c7a::FunctionStack;
    std::vector<double> elements;

    // User-defined functions
    auto fmap_fn =
        [=](double input, auto emit_func) {
            emit_func(input);
            emit_func(input);
        };

    auto map_fn =
        [=](double input) {
            return 2 * input;
        };

    auto filter_fn =
        [=](double input) {
            return input > 80;
        };

    double total = 0;

    auto save_fn =
        [&total](double input) {
        //elements.push_back(input);
            total += input;
        };

    // Converted emitter functions
    auto conv_map_fn =
        [=](double input, auto emit_func) {
            emit_func(map_fn(input));
        };

    auto conv_filter_fn =
        [=](double input, auto emit_func) {
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

    for (size_t i = 0; i != 1000; ++i) {
        composed_function(42);
        composed_function(2);
        composed_function(50);
    }

    std::cout << "total: " << total << std::endl;
    ASSERT_EQ(total, 368000u);
    
    return;
}

/******************************************************************************/
