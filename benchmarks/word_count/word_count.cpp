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

    auto start_func =
        [&input, &output](api::Context& ctx) {
            auto input_dia = ReadLines(ctx, input);

            std::string word;
            word.reserve(24);
            auto word_pairs = input_dia.template FlatMap<WordCountPair>(
                [&word](const std::string& line, auto emit) -> void {
                    /* map lambda: emit each word */
                    word.clear();
                    for (auto it = line.begin(); it != line.end(); it++) {
                        if (*it == ' ') {
                            emit(WordCountPair(word, 1));
                            word.clear();
                            word.reserve(40);
                        }
                        else {
                            word.push_back(*it);
                        }
                    }
                    emit(WordCountPair(word, 1));
                    word.reserve(40);
                }).ReducePair(
                [](const size_t& a, const size_t& b) {
                    /* associative reduction operator: add counters */
                    return a + b;
                });

            word_pairs.Map(
                [](const WordCountPair& wc) {
                    return wc.first + ": " + std::to_string(wc.second);
                }).WriteLinesMany(output);
        };

    return api::Run(start_func);
}

/******************************************************************************/
