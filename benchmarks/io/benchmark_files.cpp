/*******************************************************************************
 * benchmarks/io/benchmark_files.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2003 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2007-2011 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2013, 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/math.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/io/create_file.hpp>
#include <thrill/io/request_operations.hpp>
#include <thrill/mem/aligned_allocator.hpp>

#include <algorithm>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

#define POLL_DELAY 1000

#if THRILL_WINDOWS
const char* default_file_type = "wincall";
#else
const char* default_file_type = "syscall";
#endif

using Timer = common::StatsTimerStart;

#ifdef WATCH_TIMES
void watch_times(RequestPtr reqs[], unsigned n, double* out) {
    bool* finished = new bool[n];
    unsigned count = 0;
    for (unsigned i = 0; i < n; i++)
        finished[i] = false;

    while (count != n)
    {
        usleep(POLL_DELAY);
        unsigned i = 0;
        for (i = 0; i < n; i++)
        {
            if (!finished[i])
                if (reqs[i]->poll())
                {
                    finished[i] = true;
                    out[i] = timestamp();
                    count++;
                }
        }
    }
    delete[] finished;
}

void out_stat(double start, double end, double* times, unsigned n, const std::vector<std::string>& names) {
    for (unsigned i = 0; i < n; i++)
    {
        std::cout << i << " " << names[i] << " took " <<
            100. * (times[i] - start) / (end - start) << " %" << std::endl;
    }
}
#endif

#define MB (1024 * 1024)

// returns throughput in MiB/s
static inline double throughput(int64_t bytes, double seconds) {
    if (seconds == 0.0)
        return 0.0;
    return static_cast<double>(bytes) / (1024 * 1024) / seconds;
}

int main(int argc, char* argv[]) {
    uint64_t offset = 0, length = 0;

    bool no_direct_io = false;
    bool sync_io = false;
    bool resize_after_open = false;
    std::string file_type = default_file_type;
    uint64_t block_size = 0;
    unsigned int batch_size = 1;
    std::string opstr = "wv";
    unsigned pattern = 0;

    std::vector<std::string> files_arr;

    common::CmdlineParser cp;

    cp.AddParamBytes("length", length,
                     "Length to write in file.");

    cp.AddParamStringlist("filename", files_arr,
                          "File path to run benchmark on.");

    cp.AddBytes('o', "offset", offset,
                "Starting offset to write in file.");

    cp.AddFlag(0, "no-direct", no_direct_io,
               "open files without O_DIRECT");

    cp.AddFlag(0, "sync", sync_io,
               "open files with O_SYNC|O_DSYNC|O_RSYNC");

    cp.AddFlag(0, "resize", resize_after_open,
               "resize the file size after opening, "
               "needed e.g. for creating mmap files");

    cp.AddBytes(0, "block_size", block_size,
                "block size for operations (default 8 MiB)");

    cp.AddUInt(0, "batch_size", batch_size,
               "increase (default 1) to submit several I/Os at once "
               "and report average rate");

    cp.AddString('f', "file-type", file_type,
                 "Method to open file (syscall|mmap|wincall|boostfd|...) "
                 "default: " + file_type);

    cp.AddString('p', "operations", opstr,
                 "[w]rite pattern, [r]ead without verification, "
                 "read and [v]erify pattern (default: 'wv')");

    cp.AddUInt(0, "pattern", pattern,
               "32-bit pattern to write (default: block index)");

    cp.SetDescription(
        "Open a file using one of Thrill's file abstractions and perform "
        "write/read/verify tests on the file. "
        "Block sizes and batch size can be adjusted via command line. "
        "If length == 0 , then operation will continue till end of space "
        "(please ignore the write error). "
        "Memory consumption: block_size * batch_size * num_files");

    if (!cp.Process(argc, argv))
        return -1;

    uint64_t endpos = offset + length;

    if (block_size == 0)
        block_size = 8 * MB;

    if (batch_size == 0)
        batch_size = 1;

    bool do_read = false, do_write = false, do_verify = false;

    // deprecated, use --no-direct instead
    if (opstr.find("nd") != std::string::npos || opstr.find("ND") != std::string::npos) {
        no_direct_io = true;
    }

    if (opstr.find('r') != std::string::npos || opstr.find('R') != std::string::npos) {
        do_read = true;
    }
    if (opstr.find('v') != std::string::npos || opstr.find('V') != std::string::npos) {
        do_verify = true;
    }
    if (opstr.find('w') != std::string::npos || opstr.find('W') != std::string::npos) {
        do_write = true;
    }

    const char* myself = strrchr(argv[0], '/');
    if (!myself || !*(++myself))
        myself = argv[0];
    std::cout << "# " << myself;
    std::cout << std::endl;

    for (size_t ii = 0; ii < files_arr.size(); ii++)
    {
        std::cout << "# Add file: " << files_arr[ii] << std::endl;
    }

    const size_t nfiles = files_arr.size();
    bool verify_failed = false;

    const size_t step_size = block_size * batch_size;
    const size_t block_size_int = block_size / sizeof(int);
    const uint64_t step_size_int = step_size / sizeof(int);

    unsigned* buffer = reinterpret_cast<unsigned*>(mem::aligned_alloc(step_size * nfiles));
    std::vector<io::FileBasePtr> files(nfiles);
    io::RequestPtr* reqs = new io::RequestPtr[nfiles * batch_size];

#ifdef WATCH_TIMES
    double* r_finish_times = new double[nfiles];
    double* w_finish_times = new double[nfiles];
#endif

    double totaltimeread = 0, totaltimewrite = 0;
    int64_t totalsizeread = 0, totalsizewrite = 0;

    // fill buffer with pattern
    for (unsigned i = 0; i < nfiles * step_size_int; i++)
        buffer[i] = (pattern ? pattern : i);

    // open files
    for (unsigned i = 0; i < nfiles; i++)
    {
        int openmode = io::FileBase::CREAT | io::FileBase::RDWR;
        if (!no_direct_io) {
            openmode |= io::FileBase::DIRECT;
        }
        if (sync_io) {
            openmode |= io::FileBase::SYNC;
        }

        files[i] = io::CreateFile(file_type, files_arr[i], openmode, i);
        if (resize_after_open)
            files[i]->set_size(endpos);
    }

    std::cout << "# Step size: "
              << step_size << " bytes per file ("
              << batch_size << " block" << (batch_size == 1 ? "" : "s") << " of "
              << block_size << " bytes)"
              << " file_type=" << file_type
              << " O_DIRECT=" << (no_direct_io ? "no" : "yes")
              << " O_SYNC=" << (sync_io ? "yes" : "no")
              << std::endl;

    Timer t_total;
    try {
        while (offset + uint64_t(step_size) <= endpos || length == 0)
        {
            const uint64_t current_step_size = (length == 0) ? int64_t(step_size) : std::min<int64_t>(step_size, endpos - offset);
            const uint64_t current_step_size_int = current_step_size / sizeof(int);
            const size_t current_num_blocks =
                (size_t)common::IntegerDivRoundUp<uint64_t>(current_step_size, block_size);

            std::cout << "File offset    " << std::setw(8) << offset / MB << " MiB: " << std::fixed;

            double elapsed;
            Timer t_run;

            if (do_write)
            {
                // write block number (512 byte blocks) into each block at position 42 * sizeof(unsigned)
                for (uint64_t j = 42, b = offset >> 9; j < current_step_size_int; j += 512 / sizeof(unsigned), ++b)
                {
                    for (unsigned i = 0; i < nfiles; i++)
                        buffer[current_step_size_int * i + j] = (unsigned int)b;
                }

                for (unsigned i = 0; i < nfiles; i++)
                {
                    for (size_t j = 0; j < current_num_blocks; j++)
                        reqs[i * current_num_blocks + j] =
                            files[i]->awrite(buffer + current_step_size_int * i + j * block_size_int,
                                             offset + j * block_size,
                                             (size_t)block_size);
                }

 #ifdef WATCH_TIMES
                watch_times(reqs, nfiles, w_finish_times);
 #else
                wait_all(reqs, nfiles * current_num_blocks);
 #endif

                elapsed = t_run.SecondsDouble();
                totalsizewrite += current_step_size;
                totaltimewrite += elapsed;
            }
            else {
                elapsed = 0.0;
            }

#if 0
            std::cout << "# WRITE\nFiles: " << nfiles
                      << " \nElapsed time: " << end - begin
                      << " \nThroughput: " << int(double(current_step_size * nfiles) / MB / (end - begin))
                      << " MiB/s \nPer one file:"
                      << int(double(current_step_size) / MB / (end - begin)) << " MiB/s"
                      << std::endl;
#endif

 #ifdef WATCH_TIMES
            out_stat(begin, end, w_finish_times, nfiles, files_arr);
 #endif
            std::cout << std::setw(2) << nfiles << " * "
                      << std::setw(8) << std::setprecision(3)
                      << (throughput(current_step_size, elapsed)) << " = "
                      << std::setw(8) << std::setprecision(3)
                      << (throughput(current_step_size, elapsed) * static_cast<double>(nfiles)) << " MiB/s write,";

            t_run.Reset();

            if (do_read || do_verify)
            {
                for (unsigned i = 0; i < nfiles; i++)
                {
                    for (unsigned j = 0; j < current_num_blocks; j++)
                        reqs[i * current_num_blocks + j] =
                            files[i]->aread(buffer + current_step_size_int * i + j * block_size_int,
                                            offset + j * block_size,
                                            (size_t)block_size);
                }

 #ifdef WATCH_TIMES
                watch_times(reqs, nfiles, r_finish_times);
 #else
                wait_all(reqs, nfiles * current_num_blocks);
 #endif

                elapsed = t_run.SecondsDouble();
                totalsizeread += current_step_size;
                totaltimeread += elapsed;
            }
            else {
                elapsed = 0.0;
            }

#if 0
            std::cout << "# READ\nFiles: " << nfiles
                      << " \nElapsed time: " << end - begin
                      << " \nThroughput: " << int(double(current_step_size * nfiles) / MB / (end - begin))
                      << " MiB/s \nPer one file:"
                      << int(double(current_step_size) / MB / (end - begin)) << " MiB/s"
                      << std::endl;
#endif

            std::cout << std::setw(2) << nfiles << " * "
                      << std::setw(8) << std::setprecision(3)
                      << (throughput(current_step_size, elapsed)) << " = "
                      << std::setw(8) << std::setprecision(3)
                      << (throughput(current_step_size, elapsed) * static_cast<double>(nfiles)) << " MiB/s read";

#ifdef WATCH_TIMES
            out_stat(begin, end, r_finish_times, nfiles, files_arr);
#endif

            if (do_verify)
            {
                for (unsigned d = 0; d < nfiles; ++d)
                {
                    for (unsigned s = 0; s < (current_step_size >> 9); ++s) {
                        uint64_t i = d * current_step_size_int + s * (512 / sizeof(unsigned)) + 42;
                        uint64_t b = (offset >> 9) + s;
                        if (buffer[i] != b)
                        {
                            verify_failed = true;
                            std::cout << "Error on file " << d << " sector " << std::hex << std::setw(8) << b
                                      << " got: " << std::hex << std::setw(8) << buffer[i] << " wanted: " << std::hex << std::setw(8) << b
                                      << std::dec << std::endl;
                        }
                        buffer[i] = (pattern ? pattern : (unsigned int)i);
                    }
                }

                for (uint64_t i = 0; i < nfiles * current_step_size_int; i++)
                {
                    if (buffer[i] != (pattern ? pattern : i))
                    {
                        int64_t ibuf = i / current_step_size_int;
                        uint64_t pos = i % current_step_size_int;

                        std::cout << std::endl
                                  << "Error on file " << ibuf << " position " << std::hex << std::setw(8) << offset + pos * sizeof(int)
                                  << "  got: " << std::hex << std::setw(8) << buffer[i] << " wanted: " << std::hex << std::setw(8) << i
                                  << std::dec << std::endl;

                        i = (ibuf + 1) * current_step_size_int; // jump to next

                        verify_failed = true;
                    }
                }
            }
            std::cout << std::endl;

            offset += current_step_size;
        }
    }
    catch (const std::exception& ex)
    {
        std::cout << std::endl;
        LOG1 << ex.what();
    }
    t_total.Stop();

    std::cout << "=============================================================================================" << std::endl;
    // the following line of output is parsed by misc/filebench-avgplot.sh
    std::cout << "# Average over " << std::setw(8) << std::max(totalsizewrite, totalsizeread) / MB << " MiB: ";
    std::cout << std::setw(2) << nfiles << " * "
              << std::setw(8) << std::setprecision(3)
              << (throughput(totalsizewrite, totaltimewrite)) << " = "
              << std::setw(8) << std::setprecision(3)
              << (throughput(totalsizewrite, totaltimewrite) * static_cast<double>(nfiles)) << " MiB/s write,";

    std::cout << std::setw(2) << nfiles << " * "
              << std::setw(8) << std::setprecision(3)
              << (throughput(totalsizeread, totaltimeread)) << " = "
              << std::setw(8) << std::setprecision(3)
              << (throughput(totalsizeread, totaltimeread) * static_cast<double>(nfiles)) << " MiB/s read"
              << std::endl;

    if (totaltimewrite != 0.0)
        std::cout << "# Write time   " << std::setw(8) << std::setprecision(3) << totaltimewrite << " s" << std::endl;
    if (totaltimeread != 0.0)
        std::cout << "# Read time    " << std::setw(8) << std::setprecision(3) << totaltimeread << " s" << std::endl;

    std::cout << "# Non-I/O time " << std::setw(8) << std::setprecision(3)
              << (t_total.SecondsDouble() - totaltimewrite - totaltimeread) << " s, average throughput "
              << std::setw(8) << std::setprecision(3)
              << (throughput(totalsizewrite + totalsizeread, t_total.SecondsDouble() - totaltimewrite - totaltimeread) * static_cast<double>(nfiles)) << " MiB/s"
              << std::endl;

    std::cout << "# Total time   " << std::setw(8) << std::setprecision(3) << t_total.SecondsDouble() << " s, average throughput "
              << std::setw(8) << std::setprecision(3)
              << (throughput(totalsizewrite + totalsizeread, t_total.SecondsDouble()) * static_cast<double>(nfiles)) << " MiB/s"
              << std::endl;

    if (do_verify)
    {
        std::cout << "# Verify: " << (verify_failed ? "FAILED." : "all okay.") << std::endl;
    }

#ifdef WATCH_TIMES
    delete[] r_finish_times;
    delete[] w_finish_times;
#endif
    delete[] reqs;
    files.clear();
    mem::aligned_dealloc(buffer, step_size * nfiles);

    return (verify_failed ? 1 : 0);
}

/******************************************************************************/
