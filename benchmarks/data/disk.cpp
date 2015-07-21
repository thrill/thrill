/*******************************************************************************
 * examples/bench.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <tobias.sturm@student.kit.edu>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/read.hpp>
#include <c7a/api/write.hpp>
#include <c7a/common/cmdline_parser.hpp>

#include <iostream>

using namespace c7a;

int main(int argc, const char** argv) {
    core::JobManager jobMan;
    jobMan.Connect(0, net::Endpoint::ParseEndpointList("127.0.0.1:8000"), 1);
    //data::Manager& manager = jobMan.data_manager();
    api::Context ctx(jobMan, 0);
    common::GetThreadDirectory().NameThisThread("benchmark");

    common::CmdlineParser clp;
    clp.SetDescription("c7a::data benchmark for disk I/O");
    clp.SetAuthor("Tobias Sturm <tobias.sturm@student.kit.edu>");
    std::string input_file, output_file;
    clp.AddParamString("i", input_file, "Input file");
    clp.AddParamString("o", output_file, "Output file");
    if (!clp.Process(argc, argv)) return -1;

    auto overall_timer = ctx.stats().CreateTimer("overall", "", true);

    auto lines = ReadLines(ctx, input_file, [](const std::string& line) { return line; });

    lines.WriteToFileSystem(output_file);

    overall_timer->Stop();
}

/******************************************************************************/
