#include "gtest/gtest.h"
#include "c7a/api/dia.hpp"
#include "c7a/api/context.hpp"

TEST(DIASimple, InputTest1Read) {
     auto read_int = [](std::string line) { return std::stoi(line); };
     
     Context ctx;

     auto initial = ctx.ReadFromFileSystem("tests/inputs/test1", read_int);

     assert(initial.Size() == 4);
}

TEST(DIASimple, InputTest1Write) {

     auto read_int = [](std::string line) { return std::stoi(line); };     
     auto write_int = [](int element) { return element; };
     
     Context ctx;

     auto initial = ctx.ReadFromFileSystem("tests/inputs/test1", read_int);
     ctx.WriteToFileSystem(initial, "tests/inputs/test1_result", write_int);     
     auto copy = ctx.ReadFromFileSystem("tests/inputs/test1_result", read_int);

     assert(copy.Size() == 4);
}
