/*******************************************************************************
 * benchmarks/io/benchmark_disks.cpp
 *
 * This programm will benchmark the disks configured via .stxxl disk
 * configuration files. The block manager is used to read and write blocks using
 * the different allocation strategies.
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
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
#include <thrill/mem/aligned_allocator.hpp>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

#ifdef BLOCK_ALIGN
 #undef BLOCK_ALIGN
#endif

#define BLOCK_ALIGN  4096

#define POLL_DELAY 1000

#define CHECK_AFTER_READ 0

#define KiB (1024)
#define MiB (1024 * 1024)

using Timer = common::StatsTimerStart;

template <size_t RawBlockSize, typename AllocStrategy>
int benchmark_disks_blocksize_alloc(uint64_t length, uint64_t start_offset, uint64_t batch_size,
                                    std::string optrw) {
    uint64_t endpos = start_offset + length;

    if (length == 0)
        endpos = std::numeric_limits<uint64_t>::max();

    bool do_read = (optrw.find('r') != std::string::npos);
    bool do_write = (optrw.find('w') != std::string::npos);

    // initialize disk configuration
    io::BlockManager::get_instance();

    // construct block type

    const size_t raw_block_size = RawBlockSize;
    const size_t block_size = raw_block_size / sizeof(int);

    using block_type = io::TypedBlock<raw_block_size, unsigned>;
    using BID_type = io::BID<raw_block_size>;

    if (batch_size == 0)
        batch_size = io::Config::get_instance()->disks_number();

    // calculate total bytes processed in a batch
    batch_size = raw_block_size * batch_size;

    size_t num_blocks_per_batch = (size_t)common::IntegerDivRoundUp<uint64_t>(
        batch_size, raw_block_size);
    batch_size = num_blocks_per_batch * raw_block_size;

    block_type* buffer = new block_type[num_blocks_per_batch];
    io::RequestPtr* reqs = new io::RequestPtr[num_blocks_per_batch];
    std::vector<BID_type> blocks;
    double totaltimeread = 0, totaltimewrite = 0;
    uint64_t totalsizeread = 0, totalsizewrite = 0;

    std::cout << "# Batch size: "
              << io::add_IEC_binary_multiplier(batch_size, "B") << " ("
              << num_blocks_per_batch << " blocks of "
              << io::add_IEC_binary_multiplier(raw_block_size, "B") << ")"
              << " using " << AllocStrategy().name()
              << std::endl;

    // touch data, so it is actually allcoated
    for (unsigned j = 0; j < num_blocks_per_batch; ++j)
        for (unsigned i = 0; i < block_size; ++i)
            buffer[j][i] = (unsigned)(j * block_size + i);

    try {
        AllocStrategy alloc;
        uint64_t current_batch_size;

        for (uint64_t offset = 0; offset < endpos; offset += current_batch_size)
        {
            current_batch_size = std::min<uint64_t>(batch_size, endpos - offset);
#if CHECK_AFTER_READ
            const uint64_t current_batch_size_int = current_batch_size / sizeof(int);
#endif
            const size_t current_num_blocks_per_batch =
                (size_t)common::IntegerDivRoundUp<uint64_t>(current_batch_size, raw_block_size);

            size_t num_total_blocks = blocks.size();
            blocks.resize(num_total_blocks + current_num_blocks_per_batch);
            io::BlockManager::get_instance()->new_blocks(alloc, blocks.begin() + num_total_blocks, blocks.end());

            if (offset < start_offset)
                continue;

            std::cout << "Offset    " << std::setw(7) << offset / MiB << " MiB: " << std::fixed;

            double elapsed;
            Timer t_run;

            if (do_write)
            {
                for (unsigned j = 0; j < current_num_blocks_per_batch; j++)
                    reqs[j] = buffer[j].write(blocks[num_total_blocks + j]);

                io::wait_all(reqs, current_num_blocks_per_batch);

                elapsed = t_run.SecondsDouble();
                totalsizewrite += current_batch_size;
                totaltimewrite += elapsed;
            }
            else
                elapsed = 0.0;

            std::cout << std::setw(5) << std::setprecision(1)
                      << (static_cast<double>(current_batch_size) / MiB / elapsed) << " MiB/s write, ";

            t_run.Reset();

            if (do_read)
            {
                for (unsigned j = 0; j < current_num_blocks_per_batch; j++)
                    reqs[j] = buffer[j].read(blocks[num_total_blocks + j]);

                wait_all(reqs, current_num_blocks_per_batch);

                elapsed = t_run.SecondsDouble();
                totalsizeread += current_batch_size;
                totaltimeread += elapsed;
            }
            else
                elapsed = 0.0;

            std::cout << std::setw(5) << std::setprecision(1) << (static_cast<double>(current_batch_size) / MiB / elapsed) << " MiB/s read" << std::endl;

#if CHECK_AFTER_READ
            for (unsigned j = 0; j < current_num_blocks_per_batch; j++)
            {
                for (unsigned i = 0; i < block_size; i++)
                {
                    if (buffer[j][i] != j * block_size + i)
                    {
                        int ibuf = i / current_batch_size_int;
                        int pos = i % current_batch_size_int;

                        std::cout << "Error on disk " << ibuf << " position " << std::hex << std::setw(8) << offset + pos * sizeof(int)
                                  << "  got: " << std::hex << std::setw(8) << buffer[j][i] << " wanted: " << std::hex << std::setw(8) << (j * block_size + i)
                                  << std::dec << std::endl;

                        i = (ibuf + 1) * current_batch_size_int; // jump to next
                    }
                }
            }
#endif
        }
    }
    catch (const std::exception& ex)
    {
        std::cout << std::endl;
        LOG1 << ex.what();
    }

    std::cout << "=============================================================================================" << std::endl;
    std::cout << "# Average over " << std::setw(7) << totalsizewrite / MiB << " MiB: ";
    std::cout << std::setw(5) << std::setprecision(1) << (static_cast<double>(totalsizewrite) / MiB / totaltimewrite) << " MiB/s write, ";
    std::cout << std::setw(5) << std::setprecision(1) << (static_cast<double>(totalsizeread) / MiB / totaltimeread) << " MiB/s read" << std::endl;

    delete[] reqs;
    delete[] buffer;

    return 0;
}

template <typename AllocStrategy>
int benchmark_disks_alloc(uint64_t length, uint64_t offset, uint64_t batch_size,
                          uint64_t block_size, std::string optrw) {
#define run(bs) benchmark_disks_blocksize_alloc<bs, AllocStrategy>(length, offset, batch_size, optrw)
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
                  << "Available are only powers of two from 4 KiB to 128 MiB. "
                  << "You must use 'ki' instead of 'k'." << std::endl;
#undef run

    return 0;
}

int main(int argc, char* argv[]) {
    // parse command line

    common::CmdlineParser cp;

    uint64_t length = 0, offset = 0;
    unsigned int batch_size = 0;
    uint64_t block_size = 8 * MiB;
    std::string optrw = "rw", allocstr;

    cp.AddParamBytes("size", length,
                     "Amount of data to write/read from disks (e.g. 10GiB)");
    cp.AddOptParamString(
        "r|w", optrw,
        "Only read or write blocks (default: both write and read)");
    cp.AddOptParamString(
        "alloc", allocstr,
        "Block allocation strategy: RC, SR, FR, striping. (default: RC)");

    cp.AddUInt('b', "batch", batch_size,
               "Number of blocks written/read in one batch (default: D * B)");
    cp.AddBytes('B', "block_size", block_size,
                "Size of blocks written in one syscall. (default: B = 8MiB)");
    cp.AddBytes('o', "offset", offset,
                "Starting offset of operation range. (default: 0)");

    cp.SetDescription(
        "This program will benchmark the disks configured by the standard "
        ".thrill disk configuration files mechanism. Blocks of 8 MiB are "
        "written and/or read in sequence using the block manager. The batch "
        "size describes how many blocks are written/read in one batch. The "
        "are taken from block_manager using given the specified allocation "
        "strategy. If size == 0, then writing/reading operation are done "
        "until an error occurs. ");

    if (!cp.Process(argc, argv))
        return -1;

    if (allocstr.size())
    {
        if (allocstr == "RC")
            return benchmark_disks_alloc<io::RandomCyclic>(
                length, offset, batch_size, block_size, optrw);
        if (allocstr == "SR")
            return benchmark_disks_alloc<io::SimpleRandom>(
                length, offset, batch_size, block_size, optrw);
        if (allocstr == "FR")
            return benchmark_disks_alloc<io::FullyRandom>(
                length, offset, batch_size, block_size, optrw);
        if (allocstr == "striping")
            return benchmark_disks_alloc<io::Striping>(
                length, offset, batch_size, block_size, optrw);

        std::cout << "Unknown allocation strategy '" << allocstr << "'" << std::endl;
        cp.PrintUsage();
        return -1;
    }

    return benchmark_disks_alloc<THRILL_DEFAULT_ALLOC_STRATEGY>(
        length, offset, batch_size, block_size, optrw);
}

/******************************************************************************/
