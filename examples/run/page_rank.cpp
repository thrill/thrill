/*******************************************************************************
 * examples/run/page_rank.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/page_rank.hpp>
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
            ctx.enable_consume(false);

            // read input file and create links in this format:
            //
            // url linked_url
            // url linked_url
            // url linked_url
            // ...
            auto in = ReadLines(ctx, input);

            auto res = examples::PageRank(in, iter);

            res.WriteLines(output);

            auto number_edges = in.Size();
            LOG1 << "\n"
                 << "FINISHED PAGERANK COMPUTATION"
                 << "\n"
                 << std::left << std::setfill(' ')
                //                 << std::setw(10) << "#nodes: " << number_nodes
                //                 << "\n"
                 << std::setw(10) << "#edges: " << number_edges
                 << "\n"
                 << std::setw(10) << "#iter: " << iter;

            ctx.stats_graph().BuildLayout("pagerank.out");
        };

    return api::Run(start_func);
}
/******************************************************************************/
