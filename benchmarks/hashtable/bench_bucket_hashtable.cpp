/*******************************************************************************
 * benchmarks/hashtable/bench_bucket_hashtable.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/core/reduce_pre_table.hpp>
#include <thrill/data/discard_sink.hpp>

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

    unsigned int l = 25;
    clp.AddUInt('l', "num_buckets_init_scale", "L", l,
                "Lower string length, default = 5.");

    unsigned int u = 35;
    clp.AddUInt('u', "num_buckets_resize_scale", "U", u,
                "Upper string length, default = 15.");

    unsigned int num_buckets_per_partition = 1;
    clp.AddUInt('b', "num_buckets_per_partition", "B", num_buckets_per_partition,
                "Num buckets per partition, default = 1024.");

    double max_partition_fill_rate = 0.5;
    clp.AddDouble('f', "max_partition_fill_rate", "F", max_partition_fill_rate,
                  "Open hashtable with max_partition_fill_rate, default = 0.5.");

    unsigned int max_num_blocks_table = 1;
    clp.AddUInt('n', "max_num_blocks_table", "N", max_num_blocks_table,
                "Max num blocks table, default = 1024 * 16.");

    const unsigned int target_block_size = 4 * 16;
//    clp.AddUInt('z', "target_block_size", "Z", target_block_size,
//                "Target block size, default = 1024 * 16.");

    unsigned int table_size = 500000000;
    clp.AddUInt('t', "table_size", "T", table_size,
                "Table size, default = 500000000.");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    ///////
    // strings mode
    ///////

    auto key_ex = [](std::string in) { return in; };

    auto red_fn = [](std::string in1, std::string in2) {
                      (void)in2;
                      return in1;
                  };

    static const char alphanum[] =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789";

    std::default_random_engine rng(std::random_device { } ());
    std::uniform_int_distribution<> dist(l, u);

    std::vector<std::string> strings;
    size_t current_size = 0; // size of data in byte

    while (current_size < size)
    {
        size_t length = dist(rng);
        std::string str;
        for (size_t i = 0; i < length; ++i)
        {
            str += alphanum[rand() % sizeof(alphanum)];
        }
        strings.push_back(str);
        current_size += sizeof(str) + str.capacity();
    }

    data::BlockPool block_pool(nullptr, nullptr);
    std::vector<data::DiscardSink> sinks;
    std::vector<data::DynBlockWriter> writers;
    for (size_t i = 0; i != workers; ++i) {
        sinks.emplace_back(block_pool);
        writers.emplace_back(sinks[i].GetDynWriter());
    }

    core::ReducePreTable<std::string, std::string, decltype(key_ex), decltype(red_fn), true,
                         core::PreReduceByHashKey<std::string>, std::equal_to<std::string>, target_block_size>
    table(workers, key_ex, red_fn, writers, table_size, 0.001, max_partition_fill_rate);

    common::StatsTimer<true> timer(true);

    for (size_t i = 0; i < strings.size(); i++)
    {
        table.Insert(std::move(strings[i]));
    }

    timer.Stop();

    std::cout << timer.Microseconds() << " " << table.NumFlushes() << " " << strings.size() << std::endl;

    return 0;
}

/******************************************************************************/
