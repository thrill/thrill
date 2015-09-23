/*******************************************************************************
 * benchmarks/hashtable/bench_probing_hashtable.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/core/reduce_pre_probing_table.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/discard_sink.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <iterator>
#include <numeric>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include <limits.h>
#include <stddef.h>

using IntPair = std::pair<int, int>;

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    unsigned int size = 10000000;
    clp.AddUInt('s', "size", "S", size,
                "Load in byte to be inserted");

    unsigned int workers = 100;
    clp.AddUInt('w', "workers", "W", workers,
                "Open hashtable with W workers, default = 1.");

    double max_partition_fill_rate = 0.5;
    clp.AddDouble('f', "max_partition_fill_rate", "F", max_partition_fill_rate,
                  "Open hashtable with max_partition_fill_rate, default = 0.5.");

    unsigned int byte_size = 5000000;
    clp.AddUInt('t', "max_num_items_table", "T", byte_size,
                "Table size, default = 500000000.");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    ///////
    // strings mode
    ///////

    auto key_ex = [](size_t in) { return in; };

    auto red_fn = [](size_t in1, size_t in2) {
                      (void)in2;
                      return in1;
                  };

    size_t num_items = size / sizeof(size_t);

    std::default_random_engine rng(std::random_device { } ());
    std::uniform_int_distribution<size_t> dist(1, std::numeric_limits<size_t>::max());

    data::BlockPool block_pool(nullptr);
    std::vector<data::File> sinks;
    std::vector<data::File::DynWriter> writers;
    for (size_t i = 0; i != workers; ++i) {
        sinks.emplace_back(block_pool);
        writers.emplace_back(sinks[i].GetDynWriter());
    }

    core::ReducePreProbingTable<size_t, size_t, decltype(key_ex), decltype(red_fn), true>
    table(workers, key_ex, red_fn, writers, 0, byte_size, max_partition_fill_rate);

    common::StatsTimer<true> timer(true);

    for (size_t i = 0; i < num_items; i++)
    {
        table.Insert(dist(rng));
    }

    timer.Stop();

    std::cout << timer.Microseconds() << " " << table.NumFlushes() << " " << table.NumCollisions() << std::endl;

    return 0;
}

/******************************************************************************/
