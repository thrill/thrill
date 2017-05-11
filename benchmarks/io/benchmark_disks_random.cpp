/*******************************************************************************
 * benchmarks/io/benchmark_disks_random.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/math.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/io/block_manager.hpp>
#include <thrill/io/request_operations.hpp>
#include <thrill/io/typed_block.hpp>
#include <thrill/mem/aligned_allocator.hpp>
#include <tlx/cmdline_parser.hpp>
#include <tlx/math/div_ceil.hpp>
#include <tlx/string/format_si_iec_units.hpp>

#include <algorithm>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

#define KiB (1024)
#define MiB (1024 * 1024)

struct PrintNumber {
    int n;

    explicit PrintNumber(int n) : n(n) { }

    void operator () (io::Request*, bool /* success */) {
        // std::cout << n << " " << std::flush;
    }
};

using Timer = common::StatsTimerStart;

template <unsigned BlockSize, typename AllocStrategy>
void RunTest(int64_t span, int64_t worksize,
             bool do_init, bool do_read, bool do_write) {

    const unsigned raw_block_size = BlockSize;

    using TypedBlock = io::TypedBlock<raw_block_size, unsigned>;
    using BID = io::BID<0>;

    size_t num_blocks = (size_t)tlx::div_ceil(worksize, raw_block_size);
    size_t num_blocks_in_span = (size_t)tlx::div_ceil(span, raw_block_size);

    num_blocks = std::min(num_blocks, num_blocks_in_span);
    if (num_blocks == 0) num_blocks = num_blocks_in_span;

    worksize = num_blocks * raw_block_size;

    TypedBlock* buffer = new TypedBlock;
    io::RequestPtr* reqs = new io::RequestPtr[num_blocks_in_span];
    std::vector<BID> bids;

    // touch data, so it is actually allocated
    for (unsigned i = 0; i < TypedBlock::size; ++i)
        (*buffer)[i] = i;

    try {
        AllocStrategy alloc;

        bids.resize(num_blocks_in_span);
        for (BID& b : bids) b.size = raw_block_size;
        io::BlockManager::GetInstance()->new_blocks(alloc, bids.begin(), bids.end());

        std::cout << "# Span size: "
                  << tlx::format_iec_units(span) << " ("
                  << num_blocks_in_span << " blocks of "
                  << tlx::format_iec_units(raw_block_size) << ")" << std::endl;

        std::cout << "# Work size: "
                  << tlx::format_iec_units(worksize) << " ("
                  << num_blocks << " blocks of "
                  << tlx::format_iec_units(raw_block_size) << ")" << std::endl;

        double elapsed = 0;

        if (do_init)
        {
            Timer t_run;
            std::cout << "First fill up space by writing sequentially..." << std::endl;
            for (unsigned j = 0; j < num_blocks_in_span; j++)
                reqs[j] = buffer->write(bids[j]);
            wait_all(reqs, num_blocks_in_span);
            elapsed = t_run.SecondsDouble();
            std::cout << "Written "
                      << std::setw(12) << num_blocks_in_span << " blocks in " << std::fixed << std::setw(9) << std::setprecision(2) << elapsed << " seconds: "
                      << std::setw(9) << std::setprecision(1) << (static_cast<double>(num_blocks_in_span) / elapsed) << " blocks/s "
                      << std::setw(7) << std::setprecision(1) << (static_cast<double>(num_blocks_in_span * raw_block_size) / MiB / elapsed) << " MiB/s write " << std::endl;
        }

        std::cout << "Random block access..." << std::endl;

        srand((unsigned int)time(nullptr));
        std::random_shuffle(bids.begin(), bids.end());

        if (do_read)
        {
            Timer t_run;

            for (unsigned j = 0; j < num_blocks; j++)
                reqs[j] = buffer->read(bids[j], PrintNumber(j));
            wait_all(reqs, num_blocks);

            elapsed = t_run.SecondsDouble();

            std::cout << "Read    " << num_blocks << " blocks in " << std::fixed << std::setw(5) << std::setprecision(2) << elapsed << " seconds: "
                      << std::setw(5) << std::setprecision(1) << (static_cast<double>(num_blocks) / elapsed) << " blocks/s "
                      << std::setw(5) << std::setprecision(1) << (static_cast<double>(num_blocks * raw_block_size) / MiB / elapsed) << " MiB/s read" << std::endl;
        }

        std::random_shuffle(bids.begin(), bids.end());

        if (do_write)
        {
            Timer t_run;

            for (unsigned j = 0; j < num_blocks; j++)
                reqs[j] = buffer->write(bids[j], PrintNumber(j));
            wait_all(reqs, num_blocks);

            elapsed = t_run.SecondsDouble();

            std::cout << "Written " << num_blocks << " blocks in " << std::fixed << std::setw(5) << std::setprecision(2) << elapsed << " seconds: "
                      << std::setw(5) << std::setprecision(1) << (static_cast<double>(num_blocks) / elapsed) << " blocks/s "
                      << std::setw(5) << std::setprecision(1) << (static_cast<double>(num_blocks * raw_block_size) / MiB / elapsed) << " MiB/s write " << std::endl;
        }
    }
    catch (const std::exception& ex)
    {
        std::cout << std::endl;
        LOG1 << ex.what();
    }

    delete[] reqs;
    delete buffer;

    io::BlockManager::GetInstance()->delete_blocks(bids.begin(), bids.end());
}

template <typename AllocStrategy>
int BenchmarkDisksRandomAlloc(uint64_t span, uint64_t block_size, uint64_t worksize,
                              const std::string& optirw) {
    bool do_init = (optirw.find('i') != std::string::npos);
    bool do_read = (optirw.find('r') != std::string::npos);
    bool do_write = (optirw.find('w') != std::string::npos);

#define Run(bs) RunTest<bs, AllocStrategy>( \
        span, worksize, do_init, do_read, do_write)

    if (block_size == 4 * KiB)
        Run(4 * KiB);
    else if (block_size == 8 * KiB)
        Run(8 * KiB);
    else if (block_size == 16 * KiB)
        Run(16 * KiB);
    else if (block_size == 32 * KiB)
        Run(32 * KiB);
    else if (block_size == 64 * KiB)
        Run(64 * KiB);
    else if (block_size == 128 * KiB)
        Run(128 * KiB);
    else if (block_size == 256 * KiB)
        Run(256 * KiB);
    else if (block_size == 512 * KiB)
        Run(512 * KiB);
    else if (block_size == 1 * MiB)
        Run(1 * MiB);
    else if (block_size == 2 * MiB)
        Run(2 * MiB);
    else if (block_size == 4 * MiB)
        Run(4 * MiB);
    else if (block_size == 8 * MiB)
        Run(8 * MiB);
    else if (block_size == 16 * MiB)
        Run(16 * MiB);
    else if (block_size == 32 * MiB)
        Run(32 * MiB);
    else if (block_size == 64 * MiB)
        Run(64 * MiB);
    else if (block_size == 128 * MiB)
        Run(128 * MiB);
    else
        std::cerr << "Unsupported block_size " << block_size << "." << std::endl
                  << "Available are only powers of two from 4 KiB to 128 MiB. You must use 'ki' instead of 'k'." << std::endl;
#undef Run

    return 0;
}

int main(int argc, char* argv[]) {
    // parse command line

    tlx::CmdlineParser cp;

    uint64_t span, block_size = 8 * MiB, worksize = 0;
    std::string optirw = "irw", allocstr;

    cp.add_param_bytes(
        "span", span,
        "Span of external memory to write/read to (e.g. 10GiB).");
    cp.add_opt_param_bytes(
        "block_size", block_size,
        "Size of blocks to randomly write/read (default: 8MiB).");
    cp.add_opt_param_bytes(
        "size", worksize,
        "Amount of data to operate on (e.g. 2GiB), default: whole span.");
    cp.add_opt_param_string(
        "i|r|w", optirw,
        "Operations: [i]nitialize, [r]ead, and/or [w]rite (default: all).");
    cp.add_opt_param_string(
        "alloc", allocstr,
        "Block allocation strategy: RC, SR, FR, S (default: RC).");

    cp.set_description(
        "This program will benchmark _random_ block access on the disks "
        "configured by the standard .thrill disk configuration files mechanism. "
        "Available block sizes are power of two from 4 KiB to 128 MiB. "
        "A set of three operations can be performed: sequential initialization, "
        "random reading and random writing.");

    if (!cp.process(argc, argv))
        return -1;

#define RunAlloc(Alloc) BenchmarkDisksRandomAlloc<Alloc>( \
        span, block_size, worksize, optirw)

    if (allocstr.size())
    {
        if (allocstr == "RC")
            return RunAlloc(io::RandomCyclic);
        if (allocstr == "SR")
            return RunAlloc(io::SimpleRandom);
        if (allocstr == "FR")
            return RunAlloc(io::FullyRandom);
        if (allocstr == "S")
            return RunAlloc(io::Striping);

        std::cout << "Unknown allocation strategy '" << allocstr << "'" << std::endl;
        cp.print_usage();
        return -1;
    }

    return RunAlloc(THRILL_DEFAULT_ALLOC_STRATEGY);
#undef RunAlloc
}

/******************************************************************************/
