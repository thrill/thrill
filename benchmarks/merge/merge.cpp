/*******************************************************************************
 * benchmarks/merge/merge.cpp
 *
 * Minimalistic broadcast benchmark to test different net implementations.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/merge.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/string.hpp>

#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

unsigned int size = 1000 * 1000 * 10;

//! Network benchmarking.
void merge_test(thrill::api::Context& ctx) {

    std::random_device rd;
    std::mt19937 gen(rd());

    auto merge_input1 = thrill::api::Generate(
        ctx,
        [&gen](size_t index) { return gen(); },
        size);

    auto merge_input2 = thrill::api::Generate(
        ctx,
        [&gen](size_t index) { return gen(); },
        size);

    merge_input1 = merge_input1.Sort();
    merge_input2 = merge_input2.Sort();

    auto merge_result = merge_input1.Merge(
        merge_input2, std::less<size_t>());

    auto res = merge_result.Size();
}

int main(int argc, char** argv) {

    thrill::common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    clp.AddUInt('n', "size", size,
                "Count of elements to merge");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    return thrill::api::Run(merge_test);
}

/******************************************************************************/
