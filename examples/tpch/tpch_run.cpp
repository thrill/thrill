/*******************************************************************************
 * examples/triangles/triangles_run.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/page_rank/zipf_graph_gen.hpp>
#include <examples/triangles/triangles.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

#include <algorithm>
#include <cmath>
#include <ctime>
#include <functional>
#include <utility>
#include <vector>


using namespace thrill;              // NOLINT
using LineItem = std::tuple<size_t, size_t, size_t, size_t, size_t, double,
                            double, double, char, char, time_t, time_t, time_t,
                            std::string, std::string, std::string>;
using Order = std::tuple<size_t, size_t, char, double, time_t, std::string,
                         std::string, bool, std::string>;


static size_t JoinTPCH4(
    api::Context& ctx,
    const std::vector<std::string>& input_path) {
    auto lineitems = ReadLines(ctx, input_path).Map(
        [](const std::string& input, auto emit) {

            char* end;
            std::vector<std::string> splitted = common::Split(input, '|');
            size_t orderkey = std::strtoul(splitted[0], &end, 10);
            size_t partkey = std::strtoul(splitted[1], &end, 10);
            size_t suppkey = std::strtoul(splitted[2], &end, 10);
            size_t linenumber = std::strtoul(splitted[3], &end, 10);
            size_t quantity = std::strtoul(splitted[4], &end, 10);
            double extendedprice = std::strtod(splitted[5], &end);
            double discount = std::strtod(splitted[6], &end);
            double tax = std::strtod(splitted[7], &end);
            char returnflag = splitted[8][0];
            char linestatus = splitted[9][0];


            }
        }).Keep();

    return 23;
}

static

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    std::vector<std::string> input_path;
    clp.AddParamStringlist("input", input_path,
                           "input file pattern");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    die_unless(input_path.size() == 1);

    clp.PrintResult();

    return api::Run(
        [&](api::Context& ctx) {
            ctx.enable_consume();

            JoinTPCH4(ctx, input_path);

            return 42;
        });
}

/******************************************************************************/
