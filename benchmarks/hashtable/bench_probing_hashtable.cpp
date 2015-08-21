/*******************************************************************************
 * benchmarks/hashtable/bench_probing_hashtable.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/common/cmdline_parser.hpp>
#include <c7a/common/stats_timer.hpp>
#include <c7a/core/reduce_pre_probing_table.hpp>
#include <c7a/data/block_writer.hpp>
#include <c7a/data/discard_sink.hpp>
#include <c7a/data/file.hpp>
#include <math.h>
#include <cmath>
#include <numeric>
#include <iostream>
#include <iterator>
#include <vector>
#include <random>
#include <algorithm>

using IntPair = std::pair<int, int>;

using namespace c7a;
using namespace c7a::data;

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    srand(time(NULL));

    clp.SetVerboseProcess(false);

    unsigned int size = 10000000;
    clp.AddUInt('s', "size", "S", size,
                "Load in byte to be inserted");

    unsigned int workers = 100;
    clp.AddUInt('w', "workers", "W", workers,
                "Open hashtable with W workers, default = 1.");

    unsigned int l = 5;
    clp.AddUInt('l', "num_buckets_init_scale", "L", l,
                "Lower string length, default = 5.");

    unsigned int u = 15;
    clp.AddUInt('u', "num_buckets_resize_scale", "U", u,
                "Upper string length, default = 15.");

    double max_partition_fill_rate = 1.0;
    clp.AddDouble('f', "max_partition_fill_rate", "F", max_partition_fill_rate,
                  "Open hashtable with max_partition_fill_rate, default = 0.5.");

    unsigned int num_items_per_partition = 1024 * 16;
    clp.AddUInt('i', "num_items_per_partition", "I", num_items_per_partition,
                "Num items per partition, default = 1024 * 16.");

    unsigned int table_size = 5000000;
    clp.AddUInt('t', "max_num_items_table", "T", table_size,
                "Table size, default = 500000000.");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    ///////
    // strings mode
    ///////

    auto key_ex = [](std::string in) { return in; };

    auto red_fn = [](std::string in1, std::string in2) {
        return in1;
    };

    static const char alphanum[] =
                    "abcdefghijklmnopqrstuvwxyz"
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    "0123456789";

    std::default_random_engine generator;
    std::uniform_int_distribution<> dist(l, u);


    std::vector<std::string> strings;
    size_t current_size = 0; // size of data in byte

    while (current_size < size)
    {
        size_t length = dist(generator);
        std::string str;
        for(size_t i = 0; i < length; ++i)
        {
            str += alphanum[rand() % sizeof(alphanum)];
        }
        strings.push_back(str);
        current_size += sizeof(str) + str.capacity();
    }

    //std::cout << strings.size() << std::endl;

    std::vector<data::DiscardSink> sinks(workers);
    std::vector<data::BlockWriter> writers;
    for (size_t i = 0; i != workers; ++i)
    {
        writers.emplace_back(sinks[i].GetWriter());
    }

    size_t num_slots = table_size / (2*sizeof(std::string));

    //std::cout << num_slots << std::endl;
    //std::cout << num_slots / workers << std::endl;

    core::ReducePreProbingTable<std::string, std::string, decltype(key_ex), decltype(red_fn), true>
    table(workers, key_ex, red_fn, writers, "", num_slots / workers, max_partition_fill_rate);

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
