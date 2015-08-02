/*******************************************************************************
 * benchmarks/data/disk.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/context.hpp>
#include <c7a/api/read.hpp>
#include <c7a/api/write.hpp>
#include <c7a/common/cmdline_parser.hpp>

#include <iostream>
#include <string>

using namespace c7a; // NOLINT

int main(int argc, const char** argv) {
    //data::Manager& manager = jobMan.data_manager();

    common::CmdlineParser clp;
    clp.SetDescription("c7a::data benchmark for disk I/O");
    clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");
    std::string input_file, output_file;
    clp.AddParamString("i", input_file, "Input file");
    clp.AddParamString("o", output_file, "Output file");
    if (!clp.Process(argc, argv)) return -1;

    api::ExecuteSameThread([&input_file, &output_file](api::Context& ctx) {
        auto overall_timer = ctx.stats().CreateTimer("overall", "", true);

        auto lines = ReadLines(ctx, input_file, [](const std::string& line) { return line; });

        lines.WriteToFileSystem(output_file);

        overall_timer->Stop();
    });
}

/******************************************************************************/
