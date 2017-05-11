/*******************************************************************************
 * examples/sleep/sleep.cpp
 *
 * A sleep or Hello World program to measure framework startup time.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/generate.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/logger.hpp>
#include <tlx/cmdline_parser.hpp>

#include <string>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    unsigned seconds;
    clp.add_param_unsigned("seconds", seconds, "seconds to sleep");

    if (!clp.process(argc, argv))
        return -1;

    return api::Run(
        [seconds](api::Context& ctx) {
            Generate(ctx, ctx.num_workers())
            .Map([seconds](size_t i) {
                     std::this_thread::sleep_for(std::chrono::seconds(seconds));
                     return i;
                 })
            .Size();
        });
}

/******************************************************************************/
