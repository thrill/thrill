/*******************************************************************************
 * examples/page_rank/page_rank_run.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/page_rank/page_rank.hpp>

#include <thrill/api/read_lines.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>

#include <string>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    std::string input;
    clp.AddParamString("input", input,
                       "input file pattern");

    std::string output;
    clp.AddParamString("output", output,
                       "output file pattern");

    int iter;
    clp.AddParamInt("n", iter, "Iterations");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    auto start_func =
        [&input, &output, &iter](api::Context& ctx) {
            ctx.enable_consume(false);

            thrill::common::StatsTimer<true> timer;

            // read input file and create links in this format:
            //
            // url linked_url
            // url linked_url
            // url linked_url
            // ...
            auto in = ReadLines(ctx, input);

            auto res = examples::PageRank(in, iter);

            res.WriteLines(output);

            timer.Stop();

            auto number_edges = in.Size();
            LOG1 << "\n"
                 << "FINISHED PAGERANK COMPUTATION"
                 << "\n"
                 << std::left << std::setfill(' ')
                 << std::setw(10) << "#edges: " << number_edges
                 << "\n"
                 << std::setw(10) << "#iter: " << iter
                 << "\n"
                 << std::setw(10) << "time: " << timer.Milliseconds() << "ms";

            ctx.stats_graph().BuildLayout("pagerank.out");
        };

    return api::Run(start_func);
}
/******************************************************************************/
