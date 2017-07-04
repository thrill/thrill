/*******************************************************************************
 * benchmarks/api/generate_numbers.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <tlx/cmdline_parser.hpp>

#include <limits>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    unsigned elements;
    clp.add_param_unsigned("n", elements, "Elements");

    std::string output;
    clp.add_param_string("output", output,
                         "output file pattern");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    clp.print_result();

    api::Run([&output, &elements](api::Context& ctx) {

                 std::default_random_engine generator(std::random_device { } ());
                 std::uniform_int_distribution<size_t> distribution(0, std::numeric_limits<size_t>::max());

                 std::vector<std::string> copy_enforcer;
                 Generate(ctx, elements, [&distribution, &generator](size_t) {
                              return std::to_string(distribution(generator));
                          }).WriteLines(output);
             });
}

/******************************************************************************/
