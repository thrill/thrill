/*******************************************************************************
 * benchmarks/sort/sort.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/api/dia.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/fast_string.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>

#include <string>
#include <utility>

using namespace thrill; // NOLINT

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
        common::StatsTimer<true> timer(false);

        auto in = api::ReadBinary<size_t>(ctx, input).Keep();
        in.Size();

        timer.Start();
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