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

#include <examples/word_count/random_text_writer.hpp>
#include <examples/word_count/word_count.hpp>

#include <thrill/api/generate.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;               // NOLINT
using namespace examples::word_count; // NOLINT

/******************************************************************************/
// Run methods

static void RunWordCount(
    api::Context& ctx,
    const std::vector<std::string>& input_filelist, const std::string& output) {
    ctx.enable_consume();

    auto lines = ReadLines(ctx, input_filelist);

    auto word_pairs = WordCount(lines);

    if (output.size()) {
        word_pairs
        .Map([](const WordCountPair& wc) {
                 return wc.first + ": " + std::to_string(wc.second);
             })
        .WriteLines(output);
    }
    else {
        word_pairs.Execute();
    }
}

/******************************************************************************/
// Run methods with generated input, duplicate some code since it makes the
// example easier to understand.

static void RunWordCountGenerated(
    api::Context& ctx, size_t num_words, const std::string& output) {
    ctx.enable_consume();

    std::default_random_engine rng(std::random_device { } ());

    auto lines = Generate(
        ctx, num_words / 10,
        [&](size_t /* index */) {
            return RandomTextWriterGenerate(10, rng);
        });

    auto word_pairs = WordCount(lines);

    if (output.size()) {
        word_pairs
        .Map([](const WordCountPair& wc) {
                 return wc.first + ": " + std::to_string(wc.second);
             })
        .WriteLines(output);
    }
    else {
        word_pairs.Execute();
    }
}

/******************************************************************************/

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    std::string output;
    clp.AddString('o', "output", output,
                  "output file pattern");

    std::vector<std::string> input;
    clp.AddParamStringlist("input", input,
                           "input file pattern(s)");

    bool generate = false;
    clp.AddFlag('g', "generate", generate,
                "generate random words, first file pattern "
                "specifies approximately how many.");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    return api::Run(
        [&](api::Context& ctx) {
            if (generate) {
                size_t num_words;
                if (!common::from_str<size_t>(input[0], num_words))
                    die("For generated word data, set input to the number of words.");

                RunWordCountGenerated(ctx, num_words, output);
            }
            else {
                RunWordCount(ctx, input, output);
            }
        });
}

/******************************************************************************/
