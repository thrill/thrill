/*******************************************************************************
 * benchmarks/io/string_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/api/dia.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/sort.hpp>
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

    int iterations;
    clp.AddParamInt("n", iterations, "Iterations");
    
    std::string input;
    clp.AddParamString("input", input,
                       "input file pattern");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    api::Run([&input, &iterations](api::Context& ctx) {
	for (int i = 0; i < iterations; i++) {
	  common::StatsTimer<true> timer(true);
	  size_t elem = api::ReadBinary<size_t>(ctx, input).Sort().Size();
	  timer.Stop();
	  LOG1 << "RESULT" << " time=" << timer.Milliseconds();
	  LOG1 << elem;
	}
      });
}

/******************************************************************************/
