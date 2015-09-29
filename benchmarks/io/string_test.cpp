/*******************************************************************************
 * benchmarks/io/string_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/api/dia.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/fast_string.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>

#include <string>
#include <utility>

using namespace thrill; // NOLINT

using common::FastString;

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    std::string input;
    clp.AddParamString("input", input,
                       "input file pattern");

	int iterations;
    clp.AddParamInt("n", iterations, "Iterations");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

	for (int i = 0; i < iterations; i++) {

		api::Run([&input](api::Context& ctx) {
				auto input_dia = ReadLines(ctx, input);

				common::StatsTimer<true> timer(true);
				std::string str;

				LOG1 << input_dia.template FlatMap<std::string>(
					[&str](const std::string& line, auto emit) -> void {
						/* map lambda: emit each word */
						auto last = line.begin();
						for (auto it = line.begin(); it != line.end(); it++) {
							if (*it == ' ') {
								if (it > last) {
									emit(str.assign(last, it));
								}
								last = it + 1;
							}
						}
						if (line.end() > last) {
							emit(str.assign(last, line.end()));
						}
					}).Size();
			
				timer.Stop();
				std::cout << "RESULT" << " time=" << timer.Milliseconds() << std::endl;
			
			});
	}
}

/******************************************************************************/
