/*******************************************************************************
 * benchmarks/api/read_write_lines.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/write_lines_many.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/stats_timer.hpp>

#include <iostream>
#include <string>

using namespace thrill;         // NOLINT

//! Reads and Writes line data from disk and measures time for whole process
int main(int argc, const char** argv) {

    common::CmdlineParser clp;
    clp.SetDescription("thrill::data benchmark for disk I/O");
    clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");
    std::string input_file, output_file;
    int iterations;
    clp.AddParamString("i", input_file, "Input file");
    clp.AddParamString("o", output_file, "Output file");
    clp.AddParamInt("n", iterations, "Iterations");
    if (!clp.Process(argc, argv)) return -1;

    for (int i = 0; i < iterations; i++) {
        api::Run(
            [&input_file, &output_file](api::Context& ctx) {
                common::StatsTimerStart timer;
                auto lines = ReadLines(ctx, input_file);
                lines.WriteLinesMany(output_file);
                timer.Stop();
                std::cout << "RESULT"
                          << " input_file=" << input_file
                          << " time=" << timer
                          << std::endl;
            });
    }
}

/******************************************************************************/
