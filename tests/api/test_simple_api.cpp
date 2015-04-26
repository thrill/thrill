#include "gtest/gtest.h"
#include "c7a/api/dia.hpp"
#include "c7a/api/context.hpp"

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

TEST(DIASimple, ReduceStringEquality) {

    using c7a::DIA;
    using c7a::Context;

    auto doubles = Context().ReadFromFileSystem("tests/inputs/test1", [](std::string line) {
            return std::stod(line);
        });

    auto key_ex = [](double in) { return (int) in; };
    auto red_fn = [](double in1, double in2) { return in1 + in2; };
    auto map_fn = [](double input) {
            std::cout << "Map" << std::endl;
            return input;
        };
    auto fmap_fn = [](double input, std::function<void(double)> emit_func) {
            std::cout << "FlatMap" << std::endl;
            emit_func(input);
            emit_func(input);
        };

    auto duplicates = doubles.Map(map_fn);
    auto duplicates2 = duplicates.Map(map_fn);
    auto red_duplicates = duplicates2.Reduce(key_ex, red_fn);

    duplicates.PrintNodes();
    // auto duplicates3 = red_duplicates.Map(map_fn);
    // auto red_duplicates2 = duplicates3.Reduce(key_ex, red_fn);
}
