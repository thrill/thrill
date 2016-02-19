/*******************************************************************************
 * benchmarks/chaining/collapse_count.cpp
 *
 * Runner program for WordCount example. See thrill/examples/word_count.hpp for
 * the source to the example.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/collapse.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stat_logger.hpp>
#include <thrill/common/stats_timer.hpp>

#include <benchmarks/chaining/helper.hpp>

#include <string>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    std::string input;
    clp.AddParamString("input", input,
                       "number of elements");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    size_t count = std::stoi(input);

    common::StatsTimer timer;

    auto start_func =
        [&count, &timer](api::Context& ctx) {
            auto key_value = Generate(
                ctx, [](const size_t& index) {
                    return KeyValue { index, index + 10 };
                }, count);

            timer.Start();
            // auto result = key_value.map10;
            auto result = key_value;
            for (size_t i = 0; i < 10; ++i) {
                result = result.map.Collapse();
            }
            result.Size();
            timer.Stop();
        };

    api::Run(start_func);
    STAT_NO_RANK << "took" << timer.Microseconds();
    return 0;
}

/******************************************************************************/
