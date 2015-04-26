#include "gtest/gtest.h"
#include "c7a/api/dia.hpp"
#include "c7a/api/context.hpp"
#include "c7a/api/function_stack.hpp"

TEST(DISABLED_DIASimple, InputTest1ReadInt) {
     //auto read_int = [](std::string line) { return std::stoi(line); };

     //c7a::Context ctx;

     // auto initial = ctx.ReadFromFileSystem("tests/inputs/test1", read_int);

     // assert(initial.NodeString() == "[DIANode/State:NEW/Type:i]");

     // assert(initial.Size() == 4);
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

    auto fmap_fn = [](double input, std::function<void(double)> emit_func) {
            std::cout << "FlatMap(Double)" << std::endl;
            emit_func(input);
            emit_func(input);
        };

    std::cout << "==============" << std::endl;
    std::cout << "FunctionStack" << std::endl;
    std::cout << "==============" << std::endl;
    FunctionStack<> stack;
    auto new_stack = stack.push(fmap_fn);
    auto new_stack2 = new_stack.push(fmap_fn);
    auto composed_function = new_stack2.emit();
    composed_function(42);
    return;
}
