 /*******************************************************************************
 * benchmarks/page_rank/page_rank.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/groupby_index.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/generate.hpp>
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

using thrill::DIARef;
using thrill::Context;

using namespace thrill; // NOLINT

static const bool debug = false;

int main(int argc, char* argv[]) {
    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    std::string input;
    clp.AddParamString("input", input,
                       "input file pattern");

    std::string output;
    clp.AddParamString("output", output,
                       "output file pattern");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    auto start_func =
        [&input, &output](api::Context& ctx) {
            ReadLines(ctx, input)
                .Map(
                    [](const std::string& input) {
                        auto split = thrill::common::split(input, " ");
                        if (split[0] == "p")
                            return std::string("");
                        else
                        // set base of page_id to 0
                        return split[1] + " " + split[2];
                })
                .WriteLines(output);
        };

    return api::Run(start_func);
}

/******************************************************************************/
