/*******************************************************************************
 * examples/select/select_run.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/select/select.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/context.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>

using namespace thrill;           // NOLINT
using namespace examples::select; // NOLINT

static auto RunSelect(api::Context & ctx, size_t num_elems, size_t rank, bool max) {
    auto data = Generate(ctx, num_elems).Cache();
    if (max) {
        auto result = Select(data, rank,
                             [](const auto& a, const auto& b) -> bool
                             { return a > b; });

        LOG << "Result: " << result;
        return result;
    }
    else {
        auto result = Select(data, rank);

        LOG << "Result: " << result;
        return result;
    }
}

int main(int argc, char* argv[]) {
    common::CmdlineParser clp;
    clp.SetVerboseProcess(false);

    size_t num_elems = 1024 * 1024, rank = 10;
    bool max;
    clp.AddSizeT('n', "num_elemes", num_elems, "Number of elements, default: 2^10");
    clp.AddSizeT('k', "rank", rank, "Rank to select, default: 10");
    clp.AddFlag('m', "max", max, "Select maximum, default off");

    if (!clp.Process(argc, argv)) {
        return -1;
    }
    clp.PrintResult();

    return api::Run(
        [&](api::Context& ctx) {
            RunSelect(ctx, num_elems, rank, max);
        });
}

/******************************************************************************/
