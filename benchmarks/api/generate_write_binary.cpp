/*******************************************************************************
 * benchmarks/api/generate_write_binary.cpp
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
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/fast_string.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>

#include <limits>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

using common::FastString;

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    int elements;
    clp.AddParamInt("n", elements, "Elements");

    std::string output;
    clp.AddParamString("output", output,
                       "output file pattern");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    api::Run([&output, &elements](api::Context& ctx) {

                 std::default_random_engine generator(std::random_device { } ());
                 std::uniform_int_distribution<size_t> distribution(0, std::numeric_limits<size_t>::max());

                 std::vector<FastString> copy_enforcer;
                 Generate(ctx,
                          [&distribution, &generator](size_t) {
                              return distribution(generator);
                          }, (size_t)elements).WriteBinary(output, 16 * 1024 * 1024);
             });
}

/******************************************************************************/
