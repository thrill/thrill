/*******************************************************************************
 * examples/bench.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/dia.hpp>
#include <c7a/core/reduce_pre_table.hpp>
#include <c7a/common/stats_timer.hpp>
#include <c7a/common/cmdline_parser.hpp>

using IntPair = std::pair<int, int>;

using namespace c7a;

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    net::DispatcherThread dispatcher("dispatcher");
    data::Manager manager(dispatcher);

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

    unsigned int max_num_items_per_bucket = 256;
    clp.AddUInt('b',"max_num_items_per_bucket", "B", max_num_items_per_bucket,
                "Open hashtable with max_num_items_per_bucket, default = 256.");

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

    std::vector<data::File> files(workers);
    std::vector<data::File::Writer> writers;
    for (size_t i = 0; i < workers; i++) {
        writers.emplace_back(files[i].GetWriter());
    }

    core::ReducePreTable<decltype(key_ex), decltype(red_fn), data::File::Writer>
    table(workers, num_buckets_init_scale, num_buckets_resize_scale, max_num_items_per_bucket,
          max_num_items_table, key_ex, red_fn, writers);

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
