/*******************************************************************************
 * tests/io/cancel_io_test.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2009-2011 Johannes Singler <singler@kit.edu>
 * Copyright (C) 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/io/create_file.hpp>
#include <thrill/io/file.hpp>
#include <thrill/io/request_operations.hpp>
#include <thrill/io/syscall_file.hpp>
#include <thrill/mem/aligned_alloc.hpp>

#include <cstring>
#include <vector>

using namespace thrill;

struct print_completion
{
    void operator () (io::request* ptr, bool /* success */) {
        std::cout << "Request completed: " << ptr << std::endl;
    }
};

int main(int argc, char** argv) {
    if (argc < 3)
    {
        std::cout << "Usage: " << argv[0] << " filetype tempfile" << std::endl;
        return -1;
    }

    const uint64_t size = 4 * 1024 * 1024, num_blocks = 16;
    char* buffer = static_cast<char*>(mem::aligned_alloc(size));
    memset(buffer, 0, size);

    std::unique_ptr<io::file> file(
        io::create_file(
            argv[1], argv[2],
            io::file::CREAT | io::file::RDWR | io::file::DIRECT));

    file->set_size(num_blocks * size);
    std::vector<io::request_ptr> req(num_blocks);

    // without cancelation
    std::cout << "Posting " << num_blocks << " requests." << std::endl;
    io::stats_data stats1(*io::stats::get_instance());
    unsigned i = 0;
    for ( ; i < num_blocks; i++)
        req[i] = file->awrite(buffer, i * size, size, print_completion());
    wait_all(req.begin(), req.end());
    std::cout << io::stats_data(*io::stats::get_instance()) - stats1;

    // with cancelation
    std::cout << "Posting " << num_blocks << " requests." << std::endl;
    io::stats_data stats2(*io::stats::get_instance());
    for (unsigned i = 0; i < num_blocks; i++)
        req[i] = file->awrite(buffer, i * size, size, print_completion());
    // cancel first half
    std::cout << "Canceling first " << num_blocks / 2 << " requests." << std::endl;
    size_t num_canceled = cancel_all(req.begin(), req.begin() + num_blocks / 2);
    std::cout << "Successfully canceled " << num_canceled << " requests." << std::endl;
    // cancel every second in second half
    for (unsigned i = num_blocks / 2; i < num_blocks; i += 2)
    {
        std::cout << "Canceling request " << &(*(req[i])) << std::endl;
        if (req[i]->cancel())
            std::cout << "Request canceled: " << &(*(req[i])) << std::endl;
        else
            std::cout << "Request not canceled: " << &(*(req[i])) << std::endl;
    }
    wait_all(req.begin(), req.end());
    std::cout << io::stats_data(*io::stats::get_instance()) - stats2;

    mem::aligned_dealloc(buffer);

    return 0;
}

/******************************************************************************/
