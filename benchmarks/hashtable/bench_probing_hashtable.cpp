/*******************************************************************************
 * examples/bench.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/stats_timer.hpp>
#include <c7a/common/cmdline_parser.hpp>
#include <c7a/core/reduce_pre_probing_table.hpp>
#include <c7a/data/block_writer.hpp>
#include <c7a/data/discard_sink.hpp>

using IntPair = std::pair<int, int>;

using namespace c7a;

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int /* in2 */) {
                      return in1;
                  };

    srand(time(NULL));

    clp.SetVerboseProcess(false);

    unsigned int size = 1;
    clp.AddUInt('s', "size", "S", size,
                "Fill hashtable with S random integers");

    unsigned int workers = 1;
    clp.AddUInt('w',"workers", "W", workers,
                "Open hashtable with W workers, default = 1.");

    unsigned int num_buckets_init_scale = 10;
    clp.AddUInt('i',"num_buckets_init_scale", "I", num_buckets_init_scale,
                "Open hashtable with num_buckets_init_scale, default = 10.");

    unsigned int num_buckets_resize_scale = 2;
    clp.AddUInt('r',"num_buckets_resize_scale", "R", num_buckets_resize_scale,
                "Open hashtable with num_buckets_resize_scale, default = 2.");

    unsigned int stepsize = 1;
    clp.AddUInt('p',"stepsize", "P", stepsize,
                "Open hashtable with stepsize, default = 1.");

    unsigned int max_stepsize = 10;
    clp.AddUInt('z',"max_stepsize", "Z", max_stepsize,
                "Open hashtable with max_stepsize, default = 10.");

    double max_partition_fill_ratio = 0.9;
    clp.AddDouble('f',"max_partition_fill_ratio", "F", max_partition_fill_ratio,
                "Open hashtable with max_partition_fill_ratio, default = 0.9.");

    unsigned int max_num_items_table = 1048576;
    clp.AddUInt('t',"max_num_items_table", "T", max_num_items_table,
                "Open hashtable with max_num_items_table, default = 1048576.");

    unsigned int modulo = 1000;
    clp.AddUInt('m',"modulo", modulo,
                "Open hashtable with keyspace size of M.");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    std::vector<int> elements(size);

    for (size_t i = 0; i < elements.size(); i++) {
        elements[i] = rand() % modulo;
    }

    data::DiscardSink sink;
    std::vector<data::BlockWriter> writers;
    for (size_t i = 0; i < workers; i++) {
        writers.emplace_back(&sink);
    }

    core::ReducePreProbingTable<decltype(key_ex), decltype(red_fn), data::BlockWriter>
    table(workers, num_buckets_init_scale, num_buckets_resize_scale, stepsize, max_stepsize,
          max_partition_fill_ratio, max_num_items_table, key_ex, red_fn, writers, std::make_pair(-1, -1));

    common::StatsTimer<true> timer(true);

    for (size_t i = 0; i < size; i++) {
        table.Insert(std::move(elements[i]));
    }
    table.Flush();

    timer.Stop();
    std::cout << timer.Microseconds() << std::endl;

    return 0;
}

/******************************************************************************/
