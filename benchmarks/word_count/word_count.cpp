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
 * This file has no license. Only Chuck Norris can compile it.
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

// REVIEW(an): add a using common::FastString

using WordCountPair = std::pair<common::FastString, size_t>;

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

		auto word_pairs = input_dia.template FlatMap<WordCountPair>(
			[](const std::string& line, auto emit) -> void {
				/* map lambda: emit each word */
				auto last = line.begin();
				for (auto it = line.begin(); it != line.end(); it++) {
					if (*it == ' ') {
                                                if (it > last) {
                                                    // REVIEW(an): add a method to make this shorter!
							emit(WordCountPair(common::FastString::Ref(&(*last), it - last), 1));
						}
						last = it + 1;
					}
				}
					if (line.end() > last) {
						emit(WordCountPair(common::FastString::Ref(&(*last), line.end() - last), 1));
					}
			}).ReducePair(
                [](const size_t& a, const size_t& b) {
				    return a + b;
                });

		word_pairs.Map(
			[](const WordCountPair& wc) {
				return wc.first.ToString() + ": " + std::to_string(wc.second);
			}).WriteLinesMany(output);
	};

    return api::Run(start_func);
}

/******************************************************************************/
