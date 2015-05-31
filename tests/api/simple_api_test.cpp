/*******************************************************************************
 * tests/api/simple_api_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/dia.hpp>
#include <c7a/api/context.hpp>
#include <c7a/api/function_stack.hpp>
#include <c7a/core/stage_builder.hpp>
#include <tests/c7a_tests.hpp>

#include <string>
#include <vector>

#include <gtest/gtest.h>

using namespace c7a::core;

TEST(API, SharedPtrTest) {
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
    auto ints = input.Map(map_fn);
    auto doubles = ints.Map(map_fn);
    auto red_quad = doubles.ReduceBy(key_ex).With(red_fn);

    std::vector<Stage> result;
    FindStages(red_quad.get_node(), result);
    for (auto s : result)
    {
        s.Run();
    }

    return;
}

TEST(API, TypeDeductionText) {
    using c7a::DIARef;
    using c7a::Context;

    Context ctx;

    auto to_int_fn = [](std::string in) {
                         return std::stoi(in);
                     };
    auto double_int_fn = [](int in) {
                             return 2 * in;
                         };
    auto filter_geq = [](int in) {
                          return in <= 40;
                      };
    auto key_ex = [](int in) {
                      return in % 2;
                  };
    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    auto lines = ReadFromFileSystem(
        ctx,
        g_workpath + "/inputs/test1",
        [](const std::string& line) {
            return line;
        });
    auto ints = lines.Map(to_int_fn);
    auto doubles = ints.Map(double_int_fn);
    auto filtered = doubles.Filter(filter_geq);
    auto red_quad = filtered.ReduceBy(key_ex).With(red_fn);

    std::vector<Stage> result;
    FindStages(red_quad.get_node(), result);
    for (auto s : result)
    {
        s.Run();
    }

    return;
}

TEST(API, Test1Zip) {
    auto read_int = [](std::string line) {
                        return std::stoi(line);
                    };

    auto zip_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::Context ctx;

    auto initial1 = ReadFromFileSystem(ctx, "../../tests/inputs/test1", read_int);

    auto initial2 = ReadFromFileSystem(ctx, "../../tests/inputs/test1", read_int);

    auto doubled = initial2.Map([](int in) {
                                    return 2 * in;
                                });

    auto zipped = initial1.Zip(zip_fn, doubled);

    std::vector<c7a::core::Stage> result;
    FindStages(zipped.get_node(), result);
    for (auto s : result) {
        s.Run();
    }

    return;
}

TEST(API, FunctionStackTest) {
    using c7a::FunctionStack;
    std::vector<double> elements;

    // User-defined functions
    auto fmap_fn =
        [=](double input, std::function<void(double)> emit_func) {
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

    auto save_fn =
        [&elements](double input) {
            elements.push_back(input);
        };

    // Converted emitter functions
    auto conv_map_fn =
        [=](double input, std::function<void(double)> emit_func) {
            emit_func(map_fn(input));
        };

    auto conv_filter_fn =
        [=](double input, std::function<void(double)> emit_func) {
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
