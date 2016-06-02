/*******************************************************************************
 * benchmarks/hashtable/reduce.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/fast_string.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>

#include <string>
#include <utility>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;


	size_t equal = 5;
	clp.AddParamSizeT("e", equal, "Number of equal elements reduced together");

	size_t elements;
	clp.AddParamSizeT("n", elements, "Number of elements in total.");
		
    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    api::Run([&equal, &elements](api::Context& ctx) {

			auto in = api::Generate(ctx,
									[](size_t n) {
										std::array<size_t, 128> value;
										for (size_t i = 0; i < 128; ++i) {
											value[i] = i + n;
										}
										return std::make_pair(n, value);
									}, elements).Keep();

			common::StatsTimerStart timer;
			in.ReducePair(
				[](const std::array<size_t, 128>& in1, const std::array<size_t, 128>& in2) {
					std::array<size_t, 128> value_out;
					for (size_t i = 0; i < 128; ++i) {
						value_out[i] = in1[i] + in2[i];
					}
					return value_out;
				}).Size();
			timer.Stop();

			LOG1 << "RESULT" << " benchmark=duplicates time=" << timer.Milliseconds();
		});
}

/******************************************************************************/
