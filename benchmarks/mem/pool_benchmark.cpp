/*******************************************************************************
 * benchmarks/mem/pool_benchmark.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <tlx/cmdline_parser.hpp>
#include <tlx/thread_pool.hpp>

#include <algorithm>
#include <iostream>
#include <random>
#include <string>

using namespace thrill; // NOLINT
using common::StatsTimer;
using common::StatsTimerStart;
using common::StatsTimerStopped;

/******************************************************************************/

int BenchmarkOneSize(int argc, char* argv[]) {
    mem::Pool pool;

    tlx::CmdlineParser clp;

    size_t size = 128;
    size_t iterations = 128;

    clp.add_size_t(
        's', "size", size, "size (default: 128)");
    clp.add_size_t(
        'n', "iterations", iterations, "Iterations (default: 128)");

    if (!clp.process(argc, argv)) return -1;

    std::default_random_engine rng(std::random_device { } ());
    std::deque<void*> list;

    while (iterations != 0)
    {
        size_t op = rng() % 2;

        if (op == 0) {
            // allocate a memory piece
            --iterations;
            list.emplace_back(pool.allocate(size));
        }
        else if (op == 1 && !list.empty()) {
            pool.deallocate(list.front(), size);
            list.pop_front();
        }

        if (iterations % 100 == 0)
            pool.print();
    }

    while (!list.empty()) {
        pool.deallocate(list.front(), size);
        list.pop_front();
    }

    pool.print();

    return 0;
}

/******************************************************************************/

void Usage(const char* argv0) {
    std::cout
        << "Usage: " << argv0 << " <benchmark>" << std::endl
        << std::endl
        // << "    file                - File and serialization speed" << std::endl
        // << "    blockqueue          - BlockQueue test" << std::endl
        // << "    cat_stream_1factor  - 1-factor bandwidth test using CatStream" << std::endl
        // << "    mix_stream_1factor  - 1-factor bandwidth test using MixStream" << std::endl
        // << "    cat_stream_all2all  - full bandwidth test using CatStream" << std::endl
        // << "    mix_stream_all2all  - full bandwidth test using MixStream" << std::endl
        // << "    scatter             - CatStream scatter test" << std::endl
        << std::endl;
}

int main(int argc, char* argv[]) {

    if (argc <= 1) {
        Usage(argv[0]);
        return 0;
    }

    std::string benchmark = argv[1];

    if (benchmark == "one_size") {
        return BenchmarkOneSize(argc - 1, argv + 1);
    }
    else {
        Usage(argv[0]);
        return -1;
    }
}

/******************************************************************************/
