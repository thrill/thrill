/*******************************************************************************
 * benchmarks/hashtable/bench_bucket_hashtable.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/core/reduce_pre_table.hpp>
#include <thrill/data/discard_sink.hpp>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

using IntPair = std::pair<int, int>;

using namespace c7a; // NOLINT

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int /* in2 */) {
                      return in1;
                  };

    std::default_random_engine rng({ std::random_device()() });

    clp.SetVerboseProcess(false);

    unsigned int size = pow(2, 24);
    clp.AddUInt('s', "size", "S", size,
                "Fill hashtable with S random integers");

    unsigned int workers = 100;
    clp.AddUInt('w', "workers", "W", workers,
                "Open hashtable with W workers, default = 1.");

    unsigned int num_buckets_init_scale = 1000;
    clp.AddUInt('i', "num_buckets_init_scale", "I", num_buckets_init_scale,
                "Open hashtable with num_buckets_init_scale, default = 10.");

    unsigned int num_buckets_resize_scale = 2;
    clp.AddUInt('r', "num_buckets_resize_scale", "R", num_buckets_resize_scale,
                "Open hashtable with num_buckets_resize_scale, default = 2.");

    unsigned int max_num_items_per_bucket = 128;
    clp.AddUInt('b', "max_num_items_per_bucket", "B", max_num_items_per_bucket,
                "Open hashtable with max_num_items_per_bucket, default = 256.");

    unsigned int max_num_items_table = 1048576;
    clp.AddUInt('t', "max_num_items_table", "T", max_num_items_table,
                "Open hashtable with max_num_items_table, default = 1048576.");

    unsigned int modulo = 10000;
    clp.AddUInt('m', "modulo", modulo,
                "Open hashtable with keyspace size of M.");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    std::vector<int> elements(size);

    for (size_t i = 0; i < elements.size(); i++) {
        elements[i] = rng() % modulo;
    }

    std::vector<data::DiscardSink> sinks(workers);
    std::vector<data::BlockWriter> writers;
    for (size_t i = 0; i != workers; ++i) {
        writers.emplace_back(sinks[i].GetWriter());
    }

    core::ReducePreTable<int, int, decltype(key_ex), decltype(red_fn), true>
    table(workers, key_ex, red_fn, writers, num_buckets_init_scale, num_buckets_resize_scale, max_num_items_per_bucket,
          max_num_items_table);

    common::StatsTimer<true> timer(true);

    for (size_t i = 0; i < size; i++) {
        table.Insert(std::move(elements[i]));
    }

    timer.Stop();

    std::vector<size_t> values;
    size_t sum = 0;
    for (size_t i = 0; i < table.NumPartitions(); i++) {
        size_t num = table.PartitionNumItems(i);
        values.push_back(num);
        sum += num;
    }
    double mean = static_cast<double>(sum) / static_cast<double>(table.NumPartitions());
    double sq_sum = std::inner_product(values.begin(), values.end(), values.begin(), 0.0);
    double stdev = std::sqrt(sq_sum / static_cast<double>(values.size()) - mean * mean);
    double median;
    std::sort(values.begin(), values.end());
    if (values.size() % 2 == 0)
    {
        median = (values[values.size() / 2 - 1] + values[values.size() / 2]) / 2;
    }
    else
    {
        median = values[values.size() / 2];
    }

    // table.Flush();

    std::cout << timer.Microseconds() << " " << mean << " " << median << " " << stdev << std::endl;

    return 0;
}

/******************************************************************************/
