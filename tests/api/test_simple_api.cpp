#include "gtest/gtest.h"
#include "c7a/api/dia.hpp"
#include "c7a/api/context.hpp"

TEST(DIASimple, InputTest1ReadInt) {
     auto read_int = [](std::string line) { return std::stoi(line); };

     Context ctx;

     // auto initial = ctx.ReadFromFileSystem("tests/inputs/test1", read_int);

     // assert(initial.NodeString() == "[DIANode/State:NEW/Type:i]");

     // assert(initial.Size() == 4);
}

TEST(DIASimple, InputTest1ReadDouble) {
     auto read_double = [](std::string line) { return std::stod(line); };

     Context ctx;

     // auto initial = ctx.ReadFromFileSystem("tests/inputs/test1", read_double);

     // assert(initial.NodeString() == "[DIANode/State:NEW/Type:d]");

     // assert(initial.Size() == 4);

}

TEST(DIASimple, InputTest1Write) {

     auto read_int = [](std::string line) { return std::stoi(line); };
     auto write_int = [](int element) { return element; };

     Context ctx;

     // auto initial = ctx.ReadFromFileSystem("tests/inputs/test1", read_int);
     // ctx.WriteToFileSystem(initial, "tests/inputs/test1_result", write_int);
     // auto copy = ctx.ReadFromFileSystem("tests/inputs/test1_result", read_int);

     // assert(copy.NodeString() == "[DIANode/State:NEW/Type:i]");

     // assert(copy.Size() == 4);
}

TEST(DIASimple, ReduceStringEquality) {

    DIA<double> doubles = DIA<double>::BigBang();

    auto key_ex = [](double in) { return (int) in; };
    auto red_fn = [](double in1, double in2) { return in1 + in2; };

    DIA<double> reduced_doubles = doubles.Reduce(key_ex, red_fn);

    DIA<double> reduced_doubles2 = reduced_doubles;

    std::cout << reduced_doubles.NodeString() << std::endl;

    assert(reduced_doubles.NodeString() == "[ReduceNode/Type=[d]/KeyType=[i]");
}
