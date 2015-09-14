/*******************************************************************************
 * benchmarks/data/channel.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/mem/page_mapper.hpp>

#include <iostream>
#include <random>
#include <string>
#include <tuple>


using namespace thrill; // NOLINT
using common::StatsTimer;

unsigned g_iterations = 1;
uint64_t g_allocations;

int main(int argc, const char** argv) {
    common::NameThisThread("benchmark");

    common::CmdlineParser clp;
    clp.SetDescription("thrill::data benchmark for Channel I/O");
    clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");

    clp.AddBytes('a', "allocs", g_allocations, "number of allocations");
    clp.AddUInt('n', "iterations", g_iterations, "Iterations (default: 1)");

    std::string experiment;
    clp.AddParamString("experiment", experiment,
                       "experiment: mmap, malloc");

    if (!clp.Process(argc, argv)) return -1;

    for (unsigned n = 0; n < g_iterations; n++) {
        StatsTimer<true> wall_time;
        if (experiment == "malloc") {
            std::vector<void*> malloc_allocations;
            malloc_allocations.reserve(g_allocations);
            wall_time.Start();
            for (unsigned int i = 0; i < g_allocations; i++) {
                malloc_allocations[i] = malloc(thrill::data::default_block_size);
            }
            wall_time.Stop();
            for (auto a : malloc_allocations)
                free(a);

        } else {
            std::vector<uint8_t*> mmap_allocations;
            thrill::mem::PageMapper<thrill::data::default_block_size> pm("/tmp/swapfile");
            uint32_t token;
            mmap_allocations.reserve(g_allocations);
            wall_time.Start();
            for (unsigned int i = 0; i < g_allocations; i++) {
                mmap_allocations[i] = pm.Allocate(token);
            }
            wall_time.Stop();
            for (auto a : mmap_allocations)
                pm.SwapOut(a);
        }
        std::cout
            << "RESULT"
            << " experiment=" << experiment
            << " allocations=" << g_allocations
            << " time(us)=" << wall_time.Microseconds()
            << std::endl;
    }

}

/******************************************************************************/
