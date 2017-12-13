/*******************************************************************************
 * benchmarks/api/string_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/dia.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <tlx/cmdline_parser.hpp>

#include <iostream>
#include <string>
#include <utility>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    std::string input;
    clp.add_param_string("input", input,
                         "input file pattern");

    int iterations;
    clp.add_param_int("n", iterations, "Iterations");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    clp.print_result();

    for (int i = 0; i < iterations; i++) {

        api::Run([&input](api::Context& ctx) {
                     auto input_dia = ReadLines(ctx, input);

                     common::StatsTimerStart timer;
                     std::string str;

                     LOG1 << input_dia.template FlatMap<std::string>(
                         [&str](const std::string& line, auto emit) -> void {
                             /* map lambda: emit each word */
                             auto last = line.begin();
                             for (auto it = line.begin(); it != line.end(); it++) {
                                 if (*it == ' ') {
                                     if (it > last) {
                                         emit(str.assign(last, it));
                                     }
                                     last = it + 1;
                                 }
                             }
                             if (line.end() > last) {
                                 emit(str.assign(last, line.end()));
                             }
                         }).Size();

                     timer.Stop();
                     std::cout << "RESULT" << " time=" << timer << std::endl;
                 });
    }
}

/******************************************************************************/
