/*******************************************************************************
 * benchmarks/word_count/word_count.cpp
 *
 * Runner program for WordCount example. See thrill/examples/word_count.hpp for
 * the source to the example.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/api/dia.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/thrill.hpp>

using WordCountPair = std::pair<std::string, size_t>;
using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

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

    #define map Map([](const std::string& line) { return line; })
    auto start_func =
        [&input, &output](api::Context& ctx) {
            auto input_dia = ReadLines(ctx, input);

            auto word_pairs = input_dia
                .map.map.map.map.map.map.map.map.map.map;
            word_pairs.WriteLinesMany(output);
        };

    LOG1 << "Done";

    return api::Run(start_func);
}

/******************************************************************************/
