/*******************************************************************************
 * benchmarks/data/allocate.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
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
using data::default_block_size;

unsigned g_iterations = 1;
uint64_t g_allocations;
unsigned g_swapfile_growth = 1;

int main(int argc, const char** argv) {
    common::NameThisThread("benchmark");

    common::CmdlineParser clp;
    clp.SetDescription("thrill::data benchmark for Channel I/O");
    clp.SetAuthor("Tobias Sturm <mail@tobiassturm.de>");

    clp.AddBytes('a', "allocs", g_allocations, "number of allocations");
    clp.AddUInt('g', "growth", g_swapfile_growth, "growth of swap file (default: 1)");
    clp.AddUInt('n', "iterations", g_iterations, "Iterations (default: 1)");

    std::string experiment;
    clp.AddParamString("experiment", experiment,
                       "experiment: mmap, malloc");

    if (!clp.Process(argc, argv)) return -1;

    std::vector<void*> allocations;
    allocations.reserve(g_allocations);
    for (unsigned n = 0; n < g_iterations; n++) {
        StatsTimer<true> wall_time;
        if (experiment == "malloc") {
            wall_time.Start();
            for (unsigned int i = 0; i < g_allocations; i++) {
                allocations[i] = malloc(default_block_size);
            }
            wall_time.Stop();
            for (auto a : allocations)
                free(a);
        }
        else {
            static const int permission = S_IRUSR | S_IWUSR | S_IRGRP;
            static int file_flags = O_RDWR | O_CREAT | O_TRUNC | O_LARGEFILE | O_NOATIME;
            int fd_ = open("/tmp/swapfile", file_flags, permission);
            die_unless(fd_ != -1);
            static void* addr_hint = nullptr; //we give no hint - kernel decides alone
            static const int protection_flags = PROT_READ | PROT_WRITE;
            int flags = MAP_SHARED | MAP_NORESERVE;
            size_t allocated_swapspace = 0;
            wall_time.Start();
            for (unsigned int i = 0; i < g_allocations; i++) {
                if (i >= allocated_swapspace) {
                    allocated_swapspace += g_swapfile_growth;
                    die_unless(lseek(fd_, (allocated_swapspace * default_block_size) - 1, SEEK_SET) != -1);
                    die_unless(write(fd_, "\0", 1) == 1); //expect 1byte written
                }
                off_t offset = i * default_block_size;
                allocations[i] = mmap(addr_hint, default_block_size, protection_flags, flags, fd_, offset);
            }
            wall_time.Stop();
            for (auto a : allocations)
                die_unless(munmap(a, default_block_size) == 0);
            remove("/tmp/swapfile");
        }
        std::cout
            << "RESULT"
            << " experiment=" << experiment
            << " allocations=" << g_allocations
            << " growth=" << g_swapfile_growth
            << " time(us)=" << wall_time.Microseconds()
            << std::endl;
    }
}

/******************************************************************************/
