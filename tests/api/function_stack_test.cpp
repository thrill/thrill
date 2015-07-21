/*******************************************************************************
 * tests/api/function_stack_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/function_stack.hpp>
#include <c7a/common/logger.hpp>

#include <gtest/gtest.h>

#include <string>
#include <vector>

TEST(API, FunctionStackTest) {
    using c7a::api::FunctionStack;
    using c7a::api::MakeFunctionStack;

    static const bool debug = false;

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

    LOG << "==============";
    LOG << "FunctionStack";
    LOG << "==============";
    auto new_stack = MakeFunctionStack<double>(fmap_fn);
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

TEST(API, SimpleDeductionTest) {
    using c7a::api::FunctionStack;
    using c7a::api::MakeFunctionStack;

    auto fmap_fn1 =
        [=](int input, auto emit_func) {
            emit_func(std::to_string(input));
        };

    auto fmap_fn2 =
        [=](const std::string& input, auto emit_func) {
            emit_func(input + " Hello");
            emit_func(10);
        };

    auto new_stack1 = MakeFunctionStack<int>(fmap_fn1);
    auto new_stack2 = new_stack1.push(fmap_fn2);

    std::vector<std::string> output;

    auto save_output = [&](auto) {
                           output.push_back("123");
                       };

    auto new_stack3 = new_stack2.push(save_output);
    new_stack3.emit()(42);

    ASSERT_EQ(output.size(), 2u);
    ASSERT_EQ(output[0], "123");
    ASSERT_EQ(output[1], "123");
}

/******************************************************************************/
