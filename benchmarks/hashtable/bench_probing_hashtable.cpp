/*******************************************************************************
 * benchmarks/hashtable/bench_probing_hashtable.cpp
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
#include <thrill/core/reduce_post_stage.hpp>
#include <thrill/core/reduce_pre_stage.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/discard_sink.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <climits>
#include <cmath>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
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

    std::string title = "";
    clp.AddString('t', "title", "T", title,
                  "Load in byte to be inserted");

    unsigned int size = 5000000;
    clp.AddUInt('s', "size", "S", size,
                "Load in byte to be inserted");

    unsigned int workers = 100;
    clp.AddUInt('w', "workers", "W", workers,
                "Open hashtable with W workers, default = 1.");

    double max_partition_fill_rate = 0.5;
    clp.AddDouble('f', "max_partition_fill_rate", "F", max_partition_fill_rate,
                  "Open hashtable with max_partition_fill_rate, default = 0.5.");

    double table_rate = 1.0;
    clp.AddDouble('r', "table_rate", "R", table_rate,
                  "Open hashtable with max_partition_fill_rate, default = 1.0.");

    unsigned int byte_size = 5000000;
    clp.AddUInt('m', "max_num_items_table", "M", byte_size,
                "Table size, default = 500000000.");

    static const bool full_reduce = false;

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    ///////
    // strings mode
    ///////

    api::Run([&](api::Context& ctx) {

                 auto key_ex = [](size_t in) { return in; };

                 auto red_fn = [](size_t in1, size_t in2) {
                                   (void)in2;
                                   return in1;
                               };

                 size_t num_items = size / sizeof(std::pair<size_t, size_t>);

                 std::default_random_engine rng(std::random_device { } ());
                 std::uniform_int_distribution<size_t> dist(1, std::numeric_limits<size_t>::max());

                 data::BlockPool block_pool(workers);
                 std::vector<data::File> sinks;
                 std::vector<data::File::DynWriter> writers;
                 for (size_t w = 0; w < workers; ++w) {
                     sinks.emplace_back(block_pool, w);
                     writers.emplace_back(sinks[w].GetDynWriter());
                 }

                 core::ReducePreProbingStage<size_t, size_t, size_t, decltype(key_ex), decltype(red_fn), true,
                                             core::ReduceByHashKey<size_t>,
                                             std::equal_to<size_t> >
                 table(ctx, workers, key_ex, red_fn, writers, core::ReduceByHashKey<size_t>(),
                       0, 0, byte_size);

                 common::StatsTimer<true> timer(true);

                 for (size_t i = 0; i < num_items; i++)
                 {
                     table.Insert(dist(rng));
                 }

                 table.Flush();

                 timer.Stop();

                 std::cout << "RESULT" << " benchmark=" << title << " size=" << size << " byte_size=" << byte_size << " workers="
                           << workers << " max_partition_fill_rate=" << max_partition_fill_rate
                           << " table_rate_multiplier=" << table_rate << " full_reduce=" << full_reduce << " final_reduce=true"
                           << " time=" << timer.Milliseconds() << std::endl;
             });

    return 0;
}

/******************************************************************************/
