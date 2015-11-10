/*******************************************************************************
 * examples/word_count.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/generate_from_file.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/write_lines_many.hpp>
#include <thrill/common/string.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/groupby_index.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <examples/word_count.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <iostream>

using thrill::DIA;
using thrill::Context;

using namespace thrill; // NOLINT

using WordCountPair = std::pair<std::string, size_t>;

int main(int argc, char *argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    std::string input;
    clp.AddParamString("input", input,
                       "input file pattern");

    std::string output;
    clp.AddParamString("output", output,
                       "output file pattern");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    auto start_func = [&input, &output](api::Context &ctx) {
        ctx.set_consume(true);

        auto lines = ReadLines(ctx, input);

        auto word_pairs = examples::WordCount(lines);

        word_pairs.Map([](const WordCountPair &wc) {
            return wc.first + ": " + std::to_string(wc.second);
        }).WriteLinesMany(output);
    };

    return api::Run(start_func);
}

/******************************************************************************/
