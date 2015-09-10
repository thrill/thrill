/*******************************************************************************
 * benchmarks/word_count/line_count.cpp
 *
 * Runner program for LineCount example.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    std::string input;
    clp.AddParamString("input", input,
                       "input file pattern");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    auto start_func =
        [input](api::Context& ctx) {
            size_t line_count = api::ReadLines(ctx, input).Size();
            sLOG1 << "counted" << line_count << "lines in total";
        };

    return api::Run(start_func);
}

/******************************************************************************/
