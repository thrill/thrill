/*******************************************************************************
 * benchmarks/hashtable/reduce.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/dia.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <tlx/cmdline_parser.hpp>

#include <string>
#include <utility>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    int iterations;
    clp.add_param_int("n", iterations, "Iterations");

    std::string input;
    clp.add_param_string("input", input,
                         "input file pattern");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    clp.print_result();

    api::Run([&input](api::Context& ctx) {

                 auto in = api::ReadBinary<size_t>(ctx, input).Keep();
                 in.Size();

                 common::StatsTimerStart timer;
                 in.ReduceByKey([](const size_t& in) {
                                    return in;
                                }, [](const size_t& in1, const size_t& in2) {
                                    (void)in2;
                                    return in1;
                                }).Size();
                 timer.Stop();

                 LOG1 << "RESULT" << " benchmark=reduce time=" << timer.Milliseconds();
             });
}

/******************************************************************************/
