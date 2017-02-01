/*******************************************************************************
 * benchmarks/duplicates/bench_golomb.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/math.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/core/dynamic_bitset.hpp>

#include <cmath>
#include <random>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    size_t golomb_param = 5;
    double fpr_parameter = 8;
    size_t num_elements = 1;
    size_t average_distance = 10;

    common::CmdlineParser clp;

    clp.AddSizeT('b', "golomb_param", golomb_param,
                 "Set Golomb Parameter, default: 5");

    clp.AddDouble('f', "fpr_param", fpr_parameter,
                  "Set the False Positive Rate Parameter (FPR: 1/param), default: 8");

    clp.AddSizeT('n', "elements", num_elements,
                 "Set the number of elements");

    clp.AddSizeT('d', "avg_dist", average_distance,
                 "Average distance between numbers, default: 10");

    if (!clp.Process(argc, argv))
        return -1;

    size_t space_bound =
        num_elements * (2 + common::IntegerLog2Ceil((size_t)fpr_parameter));

    core::DynamicBitset<size_t> golomb_code(space_bound, false, golomb_param);

    std::default_random_engine generator(std::random_device { } ());
    std::uniform_int_distribution<int> distribution(1, (2 * average_distance) - 1);

    common::StatsTimerStart timer;

    for (size_t i = 0; i < num_elements; ++i) {
        golomb_code.golomb_in(distribution(generator));
    }

    timer.Stop();

    std::cout
        << "RESULT"
        << " benchmark=golomb"
        << " time=" << timer.Milliseconds()
        << " bitsize=" << golomb_code.bit_size()
        << " elements=" << num_elements
        << " average_distance=" << average_distance
        << " fpr_parameter=" << fpr_parameter
        << " golomb_parameter=" << golomb_param
        << std::endl;
}

/******************************************************************************/
