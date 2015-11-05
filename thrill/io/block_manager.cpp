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
    ndisks_ = config->disks_number();
    disk_allocators_ = new disk_allocator*[ndisks_];
    disk_files_ = new file*[ndisks_];

    uint64_t total_size = 0;

    for (size_t i = 0; i < ndisks_; ++i)
    {
        disk_config& cfg = config->disk(i);

        // assign queues in order of disks.
        if (cfg.queue == file::DEFAULT_QUEUE)
            cfg.queue = i;

        try
        {
            disk_files_[i] = create_file(cfg, file::CREAT | file::RDWR, i);

            LOG1 << "Disk '" << cfg.path << "' is allocated, space: "
                 << (cfg.size) / (1024 * 1024)
                 << " MiB, I/O implementation: " << cfg.fileio_string();
        }
        catch (io_error&)
        {
            LOG1 << "Error allocating disk '" << cfg.path << "', space: "
                 << (cfg.size) / (1024 * 1024)
                 << " MiB, I/O implementation: " << cfg.fileio_string();
            throw;
        }

        total_size += cfg.size;

        disk_allocators_[i] = new disk_allocator(disk_files_[i], cfg);
    }

    if (ndisks_ > 1)
    {
        LOG1 << "In total " << ndisks_ << " disks are allocated, space: "
             << (total_size / (1024 * 1024)) << " MiB";
    }

#if STXXL_MNG_COUNT_ALLOCATION
    current_allocation_ = 0;
    total_allocation_ = 0;
    maximum_allocation_ = 0;
#endif      // STXXL_MNG_COUNT_ALLOCATION
}

block_manager::~block_manager() {
    LOG << "Block manager destructor";
    for (size_t i = ndisks_; i > 0; )
    {
        --i;
        delete disk_allocators_[i];
        delete disk_files_[i];
    }
    delete[] disk_allocators_;
    delete[] disk_files_;
}

uint64_t block_manager::get_total_bytes() const {
    uint64_t total = 0;

    for (size_t i = 0; i < ndisks_; ++i)
        total += disk_allocators_[i]->get_total_bytes();

    return total;
}

uint64_t block_manager::get_free_bytes() const {
    uint64_t total = 0;

    for (size_t i = 0; i < ndisks_; ++i)
        total += disk_allocators_[i]->get_free_bytes();

    return total;
}

} // namespace io
} // namespace thrill

/******************************************************************************/
