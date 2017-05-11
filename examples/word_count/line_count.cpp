/*******************************************************************************
 * examples/word_count/line_count.cpp
 *
 * Runner program for LineCount example.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/logger.hpp>
#include <tlx/cmdline_parser.hpp>

#include <string>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    std::string input;
    clp.add_param_string("input", input,
                         "input file pattern");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    clp.print_result();

    auto start_func =
        [input](api::Context& ctx) {
            size_t line_count = api::ReadLines(ctx, input).Size();
            sLOG1 << "counted" << line_count << "lines in total";
        };

    return api::Run(start_func);
}

/******************************************************************************/
