/*******************************************************************************
 * examples/word_count/word_count_run.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/word_count/word_count.hpp>

#include <thrill/api/read_lines.hpp>
#include <thrill/api/write_lines_many.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT
using examples::WordCountPair;
using examples::FastWordCountPair;

static void RunWordCount(
    api::Context& ctx,
    const std::vector<std::string>& input_filelist, const std::string& output) {
    ctx.enable_consume();

    auto lines = ReadLines(ctx, input_filelist);

    auto word_pairs = examples::WordCount(lines);

    if (output.size()) {
        word_pairs
        .Map([](const WordCountPair& wc) {
                 return wc.first + ": " + std::to_string(wc.second);
             })
        .WriteLinesMany(output);
    }
    else {
        word_pairs.Execute();
    }
}

static void RunFastWordCount(
    api::Context& ctx,
    const std::vector<std::string>& input_filelist, const std::string& output) {
    ctx.enable_consume();

    auto lines = ReadLines(ctx, input_filelist);

    auto word_pairs = examples::FastWordCount(lines);

    if (output.size()) {
        word_pairs
        .Map([](const FastWordCountPair& wc) {
                 return wc.first.ToString() + ": " + std::to_string(wc.second);
             })
        .WriteLinesMany(output);
    }
    else {
        word_pairs.Execute();
    }
}

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    bool use_fast_string = false;
    clp.AddFlag('f', "fast_string", use_fast_string,
                "use FastString implementation");

    std::string output;
    clp.AddString('o', "output", output,
                  "output file pattern");

    std::vector<std::string> input;
    clp.AddParamStringlist("input", input,
                           "input file pattern(s)");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    return api::Run(
        [&](api::Context& ctx) {
            if (use_fast_string)
                RunFastWordCount(ctx, input, output);
            else
                RunWordCount(ctx, input, output);
        });
}

/******************************************************************************/
