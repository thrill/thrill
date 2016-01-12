/*******************************************************************************
 * benchmarks/word_count/word_count_bench.cpp
 *
 * Runner program for WordCount example. See thrill/examples/word_count.hpp for
 * the source to the example.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/dia.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/write_lines_many.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/fast_string.hpp>
#include <thrill/common/logger.hpp>

#include <string>
#include <utility>

using namespace thrill; // NOLINT

using common::FastString;

using WordCountPair = std::pair<FastString, size_t>;

WordCountPair CreateWCPair(std::string::const_iterator start, size_t length) {
    return WordCountPair(FastString::Ref(start, length), 1);
}

template <typename InStack>
auto WordCount(const DIA<std::string, InStack>&input) {

    auto word_pairs = input.template FlatMap<WordCountPair>(
        [](const std::string& line, auto emit) -> void {
            /* map lambda: emit each word */
            thrill::common::SplitCallback(
                line, ' ', [&](const auto& begin, const auto& end) {
                    if (begin < end)
                        emit(CreateWCPair(begin, end - begin));
                });
        }).ReducePair(
        [](const size_t& a, const size_t& b) {
            return a + b;
        });

    return word_pairs.Map(
        [](const WordCountPair& wc) {
            return wc.first.ToString() + ": " + std::to_string(wc.second);
        });
}

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

    auto start_func = [&input, &output](api::Context& ctx) {
                          ctx.set_consume(true);

                          auto lines = ReadLines(ctx, input);

                          auto word_pairs = WordCount(lines);

                          word_pairs.WriteLinesMany(output);
                      };

    return api::Run(start_func);
}

/******************************************************************************/
