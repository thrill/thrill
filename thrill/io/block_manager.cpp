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
#include <thrill/io/disk_queues.hpp>
#include <thrill/io/file_base.hpp>

#include <cstddef>
#include <fstream>
#include <iostream>
#include <string>

namespace thrill {
namespace io {

class IoError;

BlockManager::BlockManager() {
    Config* config = Config::GetInstance();

    // initialize config (may read config files now)
    config->check_initialized();

    // allocate disk_allocators
    ndisks_ = config->disks_number();
    disk_allocators_.resize(ndisks_);
    disk_files_.resize(ndisks_);

    uint64_t total_size = 0;

    for (size_t i = 0; i < ndisks_; ++i)
    {
        DiskConfig& cfg = config->disk(i);

        // assign queues in order of disks.
        if (cfg.queue == FileBase::DEFAULT_QUEUE)
            cfg.queue = static_cast<int>(i);

        try
        {
            disk_files_[i] = CreateFile(
                cfg, FileBase::CREAT | FileBase::RDWR, static_cast<int>(i));

            std::cerr
                << "Thrill: disk '" << cfg.path << "' is allocated, space: "
                << (cfg.size) / (1024 * 1024)
                << " MiB, I/O implementation: " << cfg.fileio_string()
                << std::endl;
        }
        catch (IoError&)
        {
            std::cerr
                << "Thrill: ERROR allocating disk '" << cfg.path << "', space: "
                << (cfg.size) / (1024 * 1024)
                << " MiB, I/O implementation: " << cfg.fileio_string();
            throw;
        }

        total_size += cfg.size;

        // create queue for the file.
        DiskQueues::GetInstance()->MakeQueue(disk_files_[i]);

        disk_allocators_[i] = new DiskAllocator(disk_files_[i].get(), cfg);
    }

    if (ndisks_ > 1)
    {
        std::cerr
            << "Thrill: in total " << ndisks_ << " disks are allocated, space: "
            << (total_size / (1024 * 1024)) << " MiB"
            << std::endl;
    }
}

BlockManager::~BlockManager() {
    LOG << "Block manager destructor";
    for (size_t i = ndisks_; i > 0; )
    {
        --i;
        delete disk_allocators_[i];
        disk_files_[i].reset();
    }
}

uint64_t BlockManager::get_total_bytes() const {
    std::unique_lock<std::mutex> lock(mutex_);

    uint64_t total = 0;

    for (size_t i = 0; i < ndisks_; ++i)
        total += disk_allocators_[i]->total_bytes();

    return total;
}

uint64_t BlockManager::get_free_bytes() const {
    std::unique_lock<std::mutex> lock(mutex_);

    uint64_t total = 0;

    for (size_t i = 0; i < ndisks_; ++i)
        total += disk_allocators_[i]->free_bytes();

    return total;
}

} // namespace io
} // namespace thrill

/******************************************************************************/
