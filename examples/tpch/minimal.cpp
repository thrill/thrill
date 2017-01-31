/*******************************************************************************
 * examples/tpch/minimal.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/page_rank/zipf_graph_gen.hpp>
#include <examples/triangles/triangles.hpp>

#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/string.hpp>

#include <algorithm>
#include <cmath>
#include <ctime>
#include <functional>
#include <utility>
#include <vector>

using namespace thrill;              // NOLINT

int main(int, char*[]) {

    return api::Run(
        [&](api::Context& ctx) {
            ctx.enable_consume();

            auto bla2 = api::ReadLines(ctx, "s3://thrill-data/tbl/partsupp.tbl");
            auto bla3 = api::ReadLines(ctx, "s3://thrill-data/tbl/part.tbl");
            auto bla = api::ReadLines(ctx, "s3://thrill-test/");

            size_t kaputt = bla3.Size() + bla2.Size() + bla.Size();

            return kaputt;
        });
}

/******************************************************************************/
