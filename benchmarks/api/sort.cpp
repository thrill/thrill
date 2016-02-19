/*******************************************************************************
 * benchmarks/api/sort.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/fast_string.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>

#include <limits>
#include <string>
#include <utility>

using namespace thrill; // NOLINT

using common::FastString;

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    int iterations;
    clp.AddParamInt("i", iterations, "Iterations");

    uint64_t size;

    clp.AddParamBytes("size", size,
                      "Amount of data transfered between peers (example: 1 GiB).");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    api::Run(
        [&iterations, &size](api::Context& ctx) {
            for (int i = 0; i < iterations; i++) {
                std::default_random_engine generator(std::random_device { } ());
                std::uniform_int_distribution<size_t> distribution(0, std::numeric_limits<size_t>::max());

                common::StatsTimerStart timer;
                api::Generate(
                    ctx,
                    [&distribution, &generator](size_t) -> size_t {
                        return distribution(generator);
                    },
                    size / sizeof(size_t)).Sort().Size();
                timer.Stop();
                if (!ctx.my_rank()) {
                    LOG1 << "ITERATION " << i << " RESULT" << " time=" << timer.Milliseconds();
                }
            }
        });
}

/******************************************************************************/
