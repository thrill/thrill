#include "gtest/gtest.h"
#include "c7a/api/dia.hpp"
#include "c7a/api/context.hpp"
#include "c7a/api/function_stack.hpp"
#include "c7a/api/dia_base.hpp"
#include <tests/c7a-tests.hpp>
#include "c7a/engine/stage_builder.hpp"

using namespace c7a;

TEST(DIASimple, Test1Zip) {
     auto read_int = [](std::string line) { return std::stoi(line); };
     auto zip_fn = [](int in1, int in2) {
         return in1 + in2;
     };

     Context ctx;

     auto initial1 = ReadFromFileSystem(ctx, "../../tests/inputs/test1", read_int);
     auto initial2 = ReadFromFileSystem(ctx, "../../tests/inputs/test1", read_int);

     auto zipped = initial1.Zip(zip_fn, initial2);

     std::vector<c7a::engine::Stage> result;
     FindStages(zipped.get_node(), result);
    for (auto s : result)
    {
        s.Run();
        }

}

TEST(DISABLED_DIASimple, InputTest1ReadDouble) {
     //auto read_double = [](std::string line) { return std::stod(line); };

     //c7a::Context ctx;

     // auto initial = ctx.ReadFromFileSystem("tests/inputs/test1", read_double);

     // assert(initial.NodeString() == "[DIANode/State:NEW/Type:d]");

     // assert(initial.Size() == 4);

}

TEST(DISABLED_DIASimple, InputTest1Write) {

     //auto read_int = [](std::string line) { return std::stoi(line); };
     //auto write_int = [](int element) { return element; };

     //c7a::Context ctx;

     // auto initial = ctx.ReadFromFileSystem("tests/inputs/test1", read_int);
     // ctx.WriteToFileSystem(initial, "tests/inputs/test1_result", write_int);
     // auto copy = ctx.ReadFromFileSystem("tests/inputs/test1_result", read_int);

     // assert(copy.NodeString() == "[DIANode/State:NEW/Type:i]");

     // assert(copy.Size() == 4);
}

TEST(DIASimple, FunctionStackTest) {
    using c7a::FunctionStack;
    std::vector<double> elements;

    // User-defined functions
    auto fmap_fn = [=](double input, std::function<void(double)> emit_func) {
            emit_func(input);
            emit_func(input);
        };

    auto map_fn = [=](double input) {
            return 2*input;
        };

    auto filter_fn = [=](double input) {
            return input > 80;
        };

    auto save_fn = [&elements](double input) {
            elements.push_back(input);
        };

    // Converted emitter functions
    auto conv_map_fn = [=](double input, std::function<void(double)> emit_func) {
            emit_func(map_fn(input));
        };

    auto conv_filter_fn = [=](double input, std::function<void(double)> emit_func) {
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
