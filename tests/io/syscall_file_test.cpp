/*******************************************************************************
 * tests/io/syscall_file_test.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/io/mmap_file.hpp>
#include <thrill/io/request_operations.hpp>
#include <thrill/io/syscall_file.hpp>
#include <thrill/mem/aligned_allocator.hpp>

#include <cstring>
#include <limits>
#include <string>
#include <vector>

using namespace thrill;

struct my_handler
{
    void operator () (io::Request* ptr, bool /* success */) {
        LOG0 << "Request completed: " << ptr;
    }
};

int main(int argc, char** argv) {
    if (argc < 2)
    {
        LOG1 << "Usage: " << argv[0] << " tempdir";
        return -1;
    }

    std::string tempfilename[2];
    tempfilename[0] = std::string(argv[1]) + "/test_io_1.dat";
    tempfilename[1] = std::string(argv[1]) + "/test_io_2.dat";

    const int size = 1024 * 384;
    char* buffer = static_cast<char*>(mem::aligned_alloc(size));
    memset(buffer, 0, size);

#if THRILL_HAVE_MMAP_FILE
    io::FileBasePtr file1(
        new io::MmapFile(
            tempfilename[0],
            io::FileBase::CREAT | io::FileBase::RDWR | io::FileBase::DIRECT,
            0));

    file1->set_size(size * 1024);
#endif

    io::FileBasePtr file2(
        new io::SyscallFile(
            tempfilename[1],
            io::FileBase::CREAT | io::FileBase::RDWR | io::FileBase::DIRECT,
            1));

    std::vector<io::RequestPtr> req(16);
    unsigned i;
    for (i = 0; i < 16; i++)
        req[i] = file2->awrite(buffer, i * size, size, my_handler());

    io::wait_all(req.begin(), req.end());

    // check behaviour of having requests to the same location at the same time
    for (i = 2; i < 16; i++)
        req[i] = file2->awrite(buffer, 0, size, my_handler());
    req[0] = file2->aread(buffer, 0, size, my_handler());
    req[1] = file2->awrite(buffer, 0, size, my_handler());

    wait_all(req.begin(), req.end());

    mem::aligned_dealloc(buffer, size);

    LOG0 << *(io::Stats::GetInstance());

#if THRILL_HAVE_MMAP_FILE
    file1->close_remove();
#endif

    file2->close_remove();

    return 0;
}

/******************************************************************************/
