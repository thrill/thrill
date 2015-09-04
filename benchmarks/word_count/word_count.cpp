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
#include <thrill/examples/word_count.hpp>


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

    auto start_func =
        [&input, &output](api::Context& ctx) {
		auto input_dia = ReadLines(ctx, input);
		auto counted_words = examples::WordCount(input_dia);
		counted_words.Map(
        [](const WordCountPair& wc) {
            return wc.first + ": " + std::to_string(wc.second);
			}).WriteLinesMany(output);
        };

    return api::Run(start_func);
}

/******************************************************************************/
