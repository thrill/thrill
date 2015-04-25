/*******************************************************************************
 * tests/c7a/test1.cpp
 *
 * Run simple test on the Portfolio class.
 ******************************************************************************/

#include <c7a/api/dia.hpp>
#include <c7a/communication/communication_manager.hpp>
#include <c7a/communication/flow_control_channel.hpp>
#include <c7a/communication/system_control_channel.hpp>
#include <c7a/communication/blocking_channel.hpp>
#include <c7a/api/context.hpp>
#include <assert.h>

int main()
{
    auto read_int = [](std::string line) { return std::stoi(line); };
    auto write_int = [](int element) { return element; };

    Context ctx;

    auto initial = ctx.ReadFromFileSystem("../../../tests/c7a/data/test1", read_int);
    ctx.WriteToFileSystem(initial, "../../../tests/c7a/data/test1_result", write_int);

    return 0;
}

/******************************************************************************/
