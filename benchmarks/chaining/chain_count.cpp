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
#include <benchmarks/chaining/helper.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/stat_logger.hpp>

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

    auto start_func =
        [&count](api::Context& ctx) {
            auto key_value = Generate(ctx, [](const size_t& index) {
                    return KeyValue{index, index + 10};
                }, count);
            auto result = key_value.map10;
            // auto result = key_value.map.map.map.map.map.map.map.map.map.map;
            result.Size();  
        };

    common::StatsTimer<true> timer;
    timer.Start();
    api::Run(start_func);
    timer.Stop();
    STAT_NO_RANK << "took" << timer.Microseconds();

    return 0;
}

/******************************************************************************/
