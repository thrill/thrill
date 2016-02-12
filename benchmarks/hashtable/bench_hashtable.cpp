/*******************************************************************************
 * benchmarks/hashtable/bench_hashtable.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/core/reduce_by_hash_post_stage.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/discard_sink.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cmath>
#include <functional>
#include <iostream>
#include <limits>
#include <numeric>
#include <random>
#include <string>
#include <utility>
#include <vector>

using IntPair = std::pair<int, int>;

using namespace thrill; // NOLINT

std::string title;
uint64_t size = 50 * 1024 * 1024;
unsigned int workers = 100;

template <
    template <
        typename ValueType, typename Key, typename Value,
        typename KeyExtractor, typename ReduceFunction, typename Emitter,
        const bool RobustKey,
        typename IndexFunction = core::ReduceByHash<Key>,
        typename ReduceStageConfig = core::DefaultReduceTableConfig,
        typename EqualToFunction = std::equal_to<Key> >
    class HashTable>
void RunBenchmark(api::Context& ctx, core::DefaultReduceTableConfig& config) {

    auto key_ex = [](size_t in) { return in; };

    auto red_fn = [](size_t in1, size_t in2) {
                      (void)in2;
                      return in1;
                  };

    auto emit_fn = [](const IntPair&) { };

    size_t num_items = size / sizeof(std::pair<size_t, size_t>);

    std::default_random_engine rng(std::random_device { } ());
    std::uniform_int_distribution<size_t> dist(1, std::numeric_limits<size_t>::max());

    core::ReduceByHashPostStage<
        size_t, size_t, size_t,
        decltype(key_ex), decltype(red_fn), decltype(emit_fn),
        /* SendPair */ true,
        core::ReduceByHash<size_t>,
        core::DefaultReduceTableConfig,
        std::equal_to<size_t>,
        HashTable>
    table(ctx, key_ex, red_fn, emit_fn,
          core::ReduceByHash<size_t>(),
          /* sentinel */ 0,
          config);

    common::StatsTimer<true> timer(true);

    for (size_t i = 0; i < num_items; i++)
        table.Insert(dist(rng));

    table.Flush();

    timer.Stop();

    std::cout
        << "RESULT"
        << " benchmark=" << title
        << " size=" << size
        << " workers=" << workers
        << " max_partition_fill_rate=" << config.limit_partition_fill_rate()
        << " bucket_rate=" << config.bucket_rate()
        << " time=" << timer.Milliseconds()
        << std::endl;
}

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    core::DefaultReduceTableConfig config;

    std::string hashtable;

    clp.AddBytes('s', "size", "S", size,
                 "Set amount of bytes to be inserted, default = 50 MiB");

    clp.AddString('t', "title", "T", title,
                  "Load in byte to be inserted");

    clp.AddString('h', "hash-table", "H", hashtable,
                  "Set hashtable: probing or bucket");

    clp.AddUInt('w', "workers", "W", workers,
                "Open hashtable with W workers, default = 1.");

    clp.AddDouble('f', "fill_rate", "F",
                  config.limit_partition_fill_rate_,
                  "set limit_partition_fill_rate, default = 0.5.");

    clp.AddDouble('b', "bucket_rate", "B",
                  config.bucket_rate_,
                  "set bucket_rate, default = 0.5.");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    api::RunLocalSameThread(
        [&](api::Context& ctx) {
            if (hashtable == "bucket")
                return RunBenchmark<core::ReduceBucketHashTable>(ctx, config);
            else
                return RunBenchmark<core::ReduceProbingHashTable>(ctx, config);
        });

    return 0;
}

/******************************************************************************/
