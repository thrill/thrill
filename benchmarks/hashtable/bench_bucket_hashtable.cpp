/*******************************************************************************
 * benchmarks/hashtable/bench_bucket_hashtable.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/core/reduce_pre_table.hpp>
#include <thrill/core/reduce_post_table.hpp>
#include <thrill/data/discard_sink.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cmath>
#include <functional>
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

    unsigned int size = 1000000000;
    clp.AddUInt('s', "size", "S", size,
                "Load in byte to be inserted");

    unsigned int workers = 100;
    clp.AddUInt('w', "workers", "W", workers,
                "Open hashtable with W workers, default = 1.");

    double bucket_rate = 1.0;
    clp.AddDouble('b', "bucket_rate", "B", bucket_rate,
                  "bucket_rate, default = 0.5.");

    double max_partition_fill_rate = 0.5;
    clp.AddDouble('f', "max_partition_fill_rate", "F", max_partition_fill_rate,
                  "Open hashtable with max_partition_fill_rate, default = 0.5.");

    const unsigned int target_block_size = 8 * 16;
//    clp.AddUInt('z', "target_block_size", "Z", target_block_size,
//                "Target block size, default = 1024 * 16.");

    unsigned int byte_size = 1000000000;
    clp.AddUInt('t', "table_size", "T", byte_size,
                "Table size, default = 500000000.");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    ///////
    // strings mode
    ///////

    api::Run([&size, &bucket_rate, &max_partition_fill_rate, &byte_size](api::Context& ctx) {

    auto key_ex = [](size_t in) { return in; };

    auto red_fn = [](size_t in1, size_t in2) {
                      (void)in2;
                      return in1;
                  };

    size_t num_items = size / sizeof(size_t);

    std::default_random_engine rng(std::random_device { } ());
    std::uniform_int_distribution<size_t> dist(1, std::numeric_limits<size_t>::max());

//    data::BlockPool block_pool(nullptr);
//    std::vector<data::File> sinks;
//    std::vector<data::File::DynWriter> writers;
//    for (size_t i = 0; i != workers; ++i) {
//        sinks.emplace_back(block_pool);
//        writers.emplace_back(sinks[i].GetDynWriter());
//    }

    using EmitterFunction = std::function<void(const size_t&)>;
    std::vector<size_t> writer1;
    EmitterFunction emit = ([&writer1](const size_t value) {
        writer1.push_back(value);
    });

//    core::ReducePreTable<size_t, size_t, decltype(key_ex), decltype(red_fn), true,
//                         core::PreReduceByHashKey<size_t>, std::equal_to<size_t>, target_block_size>
//    table(workers, key_ex, red_fn, writers, byte_size, bucket_rate, max_partition_fill_rate);

    core::ReducePostTable<size_t, size_t, size_t, decltype(key_ex), decltype(red_fn), false,
            core::PostReduceFlushToDefault<size_t, size_t, decltype(red_fn)>,
            core::PostReduceByHashKey<size_t>, std::equal_to<size_t>, 32 * 16>
    table(ctx, key_ex, red_fn, emit, core::PostReduceByHashKey<size_t>(),
                  core::PostReduceFlushToDefault<size_t, size_t, decltype(red_fn)>(red_fn), 0, 0, 0, byte_size,
                  bucket_rate, max_partition_fill_rate, 0.01,
                  std::equal_to<size_t>());

    common::StatsTimer<true> timer(true);

    for (size_t i = 0; i < num_items; i++)
    {
        table.Insert(dist(rng));
    }

    timer.Stop();

//    std::cout << timer.Microseconds() << " " << table.NumFlushes() << " " << table.NumCollisions() << " "
//    << table.BlockFillRatesMedian() << " " << table.BlockFillRatesStedv() << " " << table.BlockLengthMedian()
//    << " " << table.BlockLengthStedv() << std::endl;

    std::cout << timer.Microseconds() << " " << table.NumSpills() << " " << std::endl;

    });

    return 0;
}

/******************************************************************************/
