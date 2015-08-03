/*******************************************************************************
 * benchmarks/data/disk.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/read.hpp>
#include <c7a/api/write.hpp>
#include <c7a/common/cmdline_parser.hpp>
#include <c7a/common/stats_timer.hpp>

#include <iostream>
#include <string>

using namespace c7a; // NOLINT

//! Reads and Writes random data from disk and measures time for whole process
int main(int argc, const char** argv) {
    using namespace c7a::common;
    core::JobManager jobMan;
    jobMan.Connect(0, net::Endpoint::ParseEndpointList("127.0.0.1:8000"), 1);
    api::Context ctx(jobMan, 0);
    common::NameThisThread("benchmark");

    CmdlineParser clp;
    clp.SetDescription("c7a::data benchmark for disk I/O");
    clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");
    std::string input_file, output_file;
    int iterations;
    clp.AddParamString("i", input_file, "Input file");
    clp.AddParamString("o", output_file, "Output file");
    clp.AddParamInt("n", iterations, "Iterations");
    if (!clp.Process(argc, argv)) return -1;

    for (int i = 0; i < iterations; i++) {
        StatsTimer<true> timer(true);
        auto lines = ReadLines(ctx, input_file);
        lines.WriteToFileSystem(output_file);
        timer.Stop();
        std::cout << "RESULT"
                  << " input_file=" << input_file
                  << " time=" << timer.Microseconds()
                  << std::endl;
    }
}

/******************************************************************************/
