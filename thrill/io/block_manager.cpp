/*******************************************************************************
 * thrill/io/block_manager.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2004 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/block_manager.hpp>
#include <thrill/io/config_file.hpp>
#include <thrill/io/create_file.hpp>
#include <thrill/io/disk_allocator.hpp>
#include <thrill/io/file.hpp>

#include <cstddef>
#include <fstream>
#include <string>

namespace thrill {
namespace io {

class io_error;

block_manager::block_manager() {
    config* config = config::get_instance();

    // initialize config (may read config files now)
    config->check_initialized();

    // allocate disk_allocators
    ndisks = config->disks_number();
    disk_allocators = new disk_allocator*[ndisks];
    disk_files = new file*[ndisks];

    uint64_t total_size = 0;

    for (unsigned i = 0; i < ndisks; ++i)
    {
        disk_config& cfg = config->disk(i);

        // assign queues in order of disks.
        if (cfg.queue == file::DEFAULT_QUEUE)
            cfg.queue = i;

        try
        {
            disk_files[i] = create_file(cfg, file::CREAT | file::RDWR, i);

            LOG << "Disk '" << cfg.path << "' is allocated, space: "
                << (cfg.size) / (1024 * 1024)
                << " MiB, I/O implementation: " << cfg.fileio_string();
        }
        catch (io_error&)
        {
            LOG << "Error allocating disk '" << cfg.path << "', space: "
                << (cfg.size) / (1024 * 1024)
                << " MiB, I/O implementation: " << cfg.fileio_string();
            throw;
        }

        total_size += cfg.size;

        disk_allocators[i] = new disk_allocator(disk_files[i], cfg);
    }

    if (ndisks > 1)
    {
        LOG << "In total " << ndisks << " disks are allocated, space: "
            << (total_size / (1024 * 1024)) << " MiB";
    }

#if STXXL_MNG_COUNT_ALLOCATION
    m_current_allocation = 0;
    m_total_allocation = 0;
    m_maximum_allocation = 0;
#endif      // STXXL_MNG_COUNT_ALLOCATION
}

block_manager::~block_manager() {
    LOG << "Block manager destructor";
    for (size_t i = ndisks; i > 0; )
    {
        --i;
        delete disk_allocators[i];
        delete disk_files[i];
    }
    delete[] disk_allocators;
    delete[] disk_files;
}

uint64_t block_manager::get_total_bytes() const {
    uint64_t total = 0;

    for (unsigned i = 0; i < ndisks; ++i)
        total += disk_allocators[i]->get_total_bytes();

    return total;
}

uint64_t block_manager::get_free_bytes() const {
    uint64_t total = 0;

    for (unsigned i = 0; i < ndisks; ++i)
        total += disk_allocators[i]->get_free_bytes();

    return total;
}

} // namespace io
} // namespace thrill

/******************************************************************************/
