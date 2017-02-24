/*******************************************************************************
 * benchmarks/api/merge.cpp
 *
 * Minimalistic broadcast benchmark to test different net implementations.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/merge.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/string.hpp>
#include <tlx/cmdline_parser.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

unsigned int size = 1000 * 1000 * 10;

//! Network benchmarking.
void merge_test(thrill::api::Context& ctx) {

    std::mt19937 gen(std::random_device { } ());

    auto merge_input1 = thrill::Generate(
        ctx, size,
        [&gen](size_t /* index */) { return gen(); });

    auto merge_input2 = thrill::Generate(
        ctx, size,
        [&gen](size_t /* index */) { return gen(); });

    merge_input1 = merge_input1.Sort().Keep();
    merge_input2 = merge_input2.Sort().Keep();

    // Force sorting of dia before we run merge.
    size_t sum = merge_input1.Sum();
    size_t sum2 = merge_input2.Sum();
    std::swap(sum, sum2);

    thrill::common::StatsTimerStart timer;

    auto merge_result = merge_input1.Merge(merge_input2);

    assert(merge_result.Size() == size * 2);
    timer.Stop();

    LOG1 << "RESULT"
         << " operation=merge"
         << " time=" << timer
         << " workers=" << ctx.num_workers();
}

int main(int argc, char** argv) {

    tlx::CmdlineParser clp;

    clp.add_unsigned('n', "size", size,
                     "Count of elements to merge");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    clp.print_result();

    return thrill::api::Run(merge_test);
}

/******************************************************************************/
