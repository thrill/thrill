/*******************************************************************************
 * benchmarks/page_rank/page_rank.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/groupby_index.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <examples/page_rank.hpp>

#include <iostream>
#include <iterator>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

using thrill::DIA;
using thrill::Context;

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
            ctx.set_consume(true);

            thrill::common::StatsTimer<true> timer(false);

            timer.Start();

            auto links = ReadLines(ctx, input);

            auto page_ranks = examples::PageRank(links, ctx, iter);

            page_ranks.WriteLines(output);

            timer.Stop();

            auto number_edges = links.Size();
            LOG1 << "\n"
                 << "FINISHED PAGERANK COMPUTATION"
                 << "\n"
                 << std::left << std::setfill(' ')
//                 << std::setw(10) << "#nodes: " << number_nodes
//                 << "\n"
                 << std::setw(10) << "#edges: " << number_edges
                 << "\n"
                 << std::setw(10) << "#iter: " << iter
                 << "\n"
                 << std::setw(10) << "time: " << timer.Milliseconds() << "ms";
        };

    return api::Run(start_func);
}