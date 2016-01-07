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

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/math.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/io/block_manager.hpp>
#include <thrill/io/request_operations.hpp>
#include <thrill/io/typed_block.hpp>
#include <thrill/mem/aligned_alloc.hpp>

#include <algorithm>
#include <ctime>
#include <iomanip>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

#define KiB (1024)
#define MiB (1024 * 1024)

struct print_number
{
    int n;

    explicit print_number(int n) : n(n) { }

    void operator () (io::request*, bool /* success */) {
        // std::cout << n << " " << std::flush;
    }
};

using Timer = common::StatsTimer<true>;

template <unsigned BlockSize, typename AllocStrategy>
void run_test(int64_t span, int64_t worksize, bool do_init, bool do_read, bool do_write) {
    const unsigned raw_block_size = BlockSize;

    using block_type = io::typed_block<raw_block_size, unsigned>;
    using BID_type = io::BID<raw_block_size>;

    size_t num_blocks =
        (size_t)common::IntegerDivRoundUp<int64_t>(worksize, raw_block_size);
    size_t num_blocks_in_span =
        (size_t)common::IntegerDivRoundUp<int64_t>(span, raw_block_size);

    num_blocks = std::min(num_blocks, num_blocks_in_span);
    if (num_blocks == 0) num_blocks = num_blocks_in_span;

    worksize = num_blocks * raw_block_size;

    block_type* buffer = new block_type;
    io::request_ptr* reqs = new io::request_ptr[num_blocks_in_span];
    std::vector<BID_type> blocks;

    // touch data, so it is actually allocated
    for (unsigned i = 0; i < block_type::size; ++i)
        (*buffer)[i] = i;

    try {
        AllocStrategy alloc;

        blocks.resize(num_blocks_in_span);
        io::block_manager::get_instance()->new_blocks(alloc, blocks.begin(), blocks.end());

        std::cout << "# Span size: "
                  << io::add_IEC_binary_multiplier(span, "B") << " ("
                  << num_blocks_in_span << " blocks of "
                  << io::add_IEC_binary_multiplier(raw_block_size, "B") << ")" << std::endl;

        std::cout << "# Work size: "
                  << io::add_IEC_binary_multiplier(worksize, "B") << " ("
                  << num_blocks << " blocks of "
                  << io::add_IEC_binary_multiplier(raw_block_size, "B") << ")" << std::endl;

        double elapsed = 0;

        if (do_init)
        {
            Timer t_run(true);
            std::cout << "First fill up space by writing sequentially..." << std::endl;
            for (unsigned j = 0; j < num_blocks_in_span; j++)
                reqs[j] = buffer->write(blocks[j]);
            wait_all(reqs, num_blocks_in_span);
            elapsed = t_run.Microseconds() / 1e6;
            std::cout << "Written "
                      << std::setw(12) << num_blocks_in_span << " blocks in " << std::fixed << std::setw(9) << std::setprecision(2) << elapsed << " seconds: "
                      << std::setw(9) << std::setprecision(1) << (static_cast<double>(num_blocks_in_span) / elapsed) << " blocks/s "
                      << std::setw(7) << std::setprecision(1) << (static_cast<double>(num_blocks_in_span * raw_block_size) / MiB / elapsed) << " MiB/s write " << std::endl;
        }

        std::cout << "Random block access..." << std::endl;

        srand((unsigned int)time(nullptr));
        std::random_shuffle(blocks.begin(), blocks.end());

        if (do_read)
        {
            Timer t_run(true);

            for (unsigned j = 0; j < num_blocks; j++)
                reqs[j] = buffer->read(blocks[j], print_number(j));
            wait_all(reqs, num_blocks);

            elapsed = t_run.Microseconds() / 1e6;

            std::cout << "Read    " << num_blocks << " blocks in " << std::fixed << std::setw(5) << std::setprecision(2) << elapsed << " seconds: "
                      << std::setw(5) << std::setprecision(1) << (static_cast<double>(num_blocks) / elapsed) << " blocks/s "
                      << std::setw(5) << std::setprecision(1) << (static_cast<double>(num_blocks * raw_block_size) / MiB / elapsed) << " MiB/s read" << std::endl;
        }

        std::random_shuffle(blocks.begin(), blocks.end());

        if (do_write)
        {
            Timer t_run(true);

            for (unsigned j = 0; j < num_blocks; j++)
                reqs[j] = buffer->write(blocks[j], print_number(j));
            wait_all(reqs, num_blocks);

            elapsed = t_run.Microseconds() / 1e6;

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

    io::block_manager::get_instance()->delete_blocks(blocks.begin(), blocks.end());
}

template <typename AllocStrategy>
int benchmark_disks_random_alloc(uint64_t span, uint64_t block_size, uint64_t worksize,
                                 const std::string& optirw) {
    bool do_init = (optirw.find('i') != std::string::npos);
    bool do_read = (optirw.find('r') != std::string::npos);
    bool do_write = (optirw.find('w') != std::string::npos);

#define run(bs) run_test<bs, AllocStrategy>(span, worksize, do_init, do_read, do_write)
    if (block_size == 4 * KiB)
        run(4 * KiB);
    else if (block_size == 8 * KiB)
        run(8 * KiB);
    else if (block_size == 16 * KiB)
        run(16 * KiB);
    else if (block_size == 32 * KiB)
        run(32 * KiB);
    else if (block_size == 64 * KiB)
        run(64 * KiB);
    else if (block_size == 128 * KiB)
        run(128 * KiB);
    else if (block_size == 256 * KiB)
        run(256 * KiB);
    else if (block_size == 512 * KiB)
        run(512 * KiB);
    else if (block_size == 1 * MiB)
        run(1 * MiB);
    else if (block_size == 2 * MiB)
        run(2 * MiB);
    else if (block_size == 4 * MiB)
        run(4 * MiB);
    else if (block_size == 8 * MiB)
        run(8 * MiB);
    else if (block_size == 16 * MiB)
        run(16 * MiB);
    else if (block_size == 32 * MiB)
        run(32 * MiB);
    else if (block_size == 64 * MiB)
        run(64 * MiB);
    else if (block_size == 128 * MiB)
        run(128 * MiB);
    else
        std::cerr << "Unsupported block_size " << block_size << "." << std::endl
                  << "Available are only powers of two from 4 KiB to 128 MiB. You must use 'ki' instead of 'k'." << std::endl;
#undef run

    return 0;
}

int main(int argc, char* argv[]) {
    // parse command line

    common::CmdlineParser cp;

    uint64_t span, block_size = 8 * MiB, worksize = 0;
    std::string optirw = "irw", allocstr;

    cp.AddParamBytes(
        "span", span,
        "Span of external memory to write/read to (e.g. 10GiB).");
    cp.AddOptParamBytes(
        "block_size", block_size,
        "Size of blocks to randomly write/read (default: 8MiB).");
    cp.AddOptParamBytes(
        "size", worksize,
        "Amount of data to operate on (e.g. 2GiB), default: whole span.");
    cp.AddOptParamString(
        "i|r|w", optirw,
        "Operations: [i]nitialize, [r]ead, and/or [w]rite (default: all).");
    cp.AddOptParamString(
        "alloc", allocstr,
        "Block allocation strategy: RC, SR, FR, striping (default: RC).");

    cp.SetDescription(
        "This program will benchmark _random_ block access on the disks "
        "configured by the standard .stxxl disk configuration files mechanism. "
        "Available block sizes are power of two from 4 KiB to 128 MiB. "
        "A set of three operations can be performed: sequential initialization, "
        "random reading and random writing.");

    if (!cp.Process(argc, argv))
        return -1;

#define run_alloc(alloc) benchmark_disks_random_alloc<alloc>(span, block_size, worksize, optirw)
    if (allocstr.size())
    {
        if (allocstr == "RC")
            return run_alloc(io::RC);
        if (allocstr == "SR")
            return run_alloc(io::SR);
        if (allocstr == "FR")
            return run_alloc(io::FR);
        if (allocstr == "striping")
            return run_alloc(io::striping);

        std::cout << "Unknown allocation strategy '" << allocstr << "'" << std::endl;
        cp.PrintUsage();
        return -1;
    }

    return run_alloc(STXXL_DEFAULT_ALLOC_STRATEGY);
#undef run_alloc
}

// vim: et:ts=4:sw=4

/******************************************************************************/
