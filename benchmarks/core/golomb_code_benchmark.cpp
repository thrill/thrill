/*******************************************************************************
 * benchmarks/core/golomb_code_benchmark.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/math.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/core/golomb_bit_stream.hpp>
#include <thrill/data/file.hpp>
#include <tlx/cmdline_parser.hpp>
#include <tlx/die.hpp>

#include <cmath>
#include <iostream>
#include <random>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    size_t golomb_param = 5;
    size_t num_elements = 1;
    size_t average_distance = 10;

    tlx::CmdlineParser clp;

    clp.add_size_t('g', "golomb_param", golomb_param,
                   "Set Golomb Parameter, default: 5");

    clp.add_size_t('n', "elements", num_elements,
                   "Set the number of elements");

    clp.add_size_t('d', "avg_dist", average_distance,
                   "Average distance between numbers, default: 10");

    if (!clp.process(argc, argv))
        return -1;

    data::BlockPool block_pool_;
    data::File file(block_pool_, 0, /* dia_id */ 0);

    uint32_t seed = std::random_device { } ();

    common::StatsTimerStopped write_timer, read_timer;

    {
        data::File::Writer fw = file.GetWriter(16);
        core::GolombBitStreamWriter<data::File::Writer> gbsw(fw, golomb_param);

        std::default_random_engine generator(seed);
        std::uniform_int_distribution<size_t> distribution(
            0, (2 * average_distance));

        write_timer.Start();

        for (size_t i = 0; i < num_elements; ++i) {
            gbsw.PutGolomb(distribution(generator));
        }

        write_timer.Stop();
    }

    size_t file_size = file.size_bytes();

    {
        data::File::Reader fr = file.GetReader(/* consume */ true);
        core::GolombBitStreamReader<data::File::Reader> gbsr(fr, golomb_param);

        std::default_random_engine generator(seed);
        std::uniform_int_distribution<size_t> distribution(
            0, (2 * average_distance));

        read_timer.Start();

        for (size_t i = 0; i < num_elements; ++i) {
            size_t a = distribution(generator), b = gbsr.GetGolomb();
            die_unequal(a, b);
        }

        read_timer.Stop();
    }

    std::cout
        << "RESULT"
        << " benchmark=golomb"
        << " write_timer=" << write_timer
        << " read_timer=" << read_timer
        << " size=" << file_size
        << " num_elements=" << num_elements
        << " average_distance=" << average_distance
        << " golomb_param=" << golomb_param
        << std::endl;
}

/******************************************************************************/
