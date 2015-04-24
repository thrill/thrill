/*******************************************************************************
 * tests/c7a/test1.cpp
 *
 * Run simple test on the Portfolio class.
 ******************************************************************************/

#include <c7a/api/dia.hpp>
#include <c7a/api/context.hpp>

int main()
{
    auto read_int = [](std::string line) { return std::stoi(line); };
    auto write_int = [](int element) { return element; };

    Context ctx;

    auto initial = ctx.ReadFromFileSystem("../../../tests/c7a/data/test1.in", read_int);
    ctx.WriteToFileSystem(initial, "../../../tests/c7a/data/test1.out", write_int);

    return 0;
}

/******************************************************************************/
