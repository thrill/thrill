/*******************************************************************************
 * tests/io/file_io_sizes_test.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2010 Johannes Singler <singler@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/io/create_file.hpp>
#include <thrill/io/file.hpp>
#include <thrill/io/iostats.hpp>
#include <thrill/io/request_operations.hpp>
#include <thrill/mem/aligned_alloc.hpp>

using namespace thrill;

int main(int argc, char** argv) {
    if (argc < 4)
    {
        LOG1 << "Usage: " << argv[0] << " filetype tempfile maxsize";
        return -1;
    }

    size_t max_size = atoi(argv[3]);
    uint64_t* buffer = static_cast<uint64_t*>(mem::aligned_alloc(max_size));

    try
    {
        std::unique_ptr<io::file> file(
            io::create_file(
                argv[1], argv[2],
                io::file::CREAT | io::file::RDWR | io::file::DIRECT));
        file->set_size(max_size);

        io::request_ptr req;
        io::stats_data stats1(*io::stats::get_instance());
        for (size_t size = 4096; size < max_size; size *= 2)
        {
            // generate data
            for (uint64_t i = 0; i < size / sizeof(uint64_t); ++i)
                buffer[i] = i;

            // write
            LOG1 << common::FormatIecUnits(size) << "B are being written at once";
            req = file->awrite(buffer, 0, size);
            wait_all(&req, 1);

            // fill with wrong data
            for (uint64_t i = 0; i < size / sizeof(uint64_t); ++i)
                buffer[i] = 0xFFFFFFFFFFFFFFFFull;

            // read again
            LOG1 << common::FormatIecUnits(size) << "B are being read at once";
            req = file->aread(buffer, 0, size);
            wait_all(&req, 1);

            // check
            bool wrong = false;
            for (uint64_t i = 0; i < size / sizeof(uint64_t); ++i) {
                if (buffer[i] != i)
                {
                    LOG1 << "Read inconsistent data at position " << i * sizeof(uint64_t);
                    wrong = true;
                    break;
                }
            }

            if (wrong)
                break;
        }
        std::cout << io::stats_data(*io::stats::get_instance()) - stats1;

        file->close_remove();
    }
    catch (io::io_error e)
    {
        std::cerr << e.what() << std::endl;
        throw;
    }

    mem::aligned_dealloc(buffer);

    return 0;
}

/******************************************************************************/
