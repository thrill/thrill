/*******************************************************************************
 * thrill/io/create_file.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2008, 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/config_file.hpp>
#include <thrill/io/create_file.hpp>
#include <thrill/io/memory_file.hpp>
#include <thrill/io/syscall_file.hpp>

#include "error_handling.hpp"

#include <ostream>
#include <stdexcept>

namespace thrill {
namespace io {

file * create_file(const std::string& io_impl,
                   const std::string& filename,
                   int options, int physical_device_id, int disk_allocator_id) {
    // construct temporary disk_config structure
    disk_config cfg(filename, 0, io_impl);
    cfg.queue = physical_device_id;
    cfg.direct =
        (options& file::REQUIRE_DIRECT) ? disk_config::DIRECT_ON :
        (options& file::DIRECT) ? disk_config::DIRECT_TRY :
        disk_config::DIRECT_OFF;

    return create_file(cfg, options, disk_allocator_id);
}

file * create_file(disk_config& cfg, int mode, int disk_allocator_id) {
    // apply disk_config settings to open mode

    mode &= ~(file::DIRECT | file::REQUIRE_DIRECT); // clear DIRECT and REQUIRE_DIRECT

    switch (cfg.direct) {
    case disk_config::DIRECT_OFF:
        break;
    case disk_config::DIRECT_TRY:
        mode |= file::DIRECT;
        break;
    case disk_config::DIRECT_ON:
        mode |= file::DIRECT | file::REQUIRE_DIRECT;
        break;
    }

    // automatically enumerate disks as separate device ids

    if (cfg.device_id == file::DEFAULT_DEVICE_ID)
    {
        cfg.device_id = config::get_instance()->get_next_device_id();
    }
    else
    {
        config::get_instance()->update_max_device_id(cfg.device_id);
    }

    // *** Select fileio Implementation

    if (cfg.io_impl == "syscall")
    {
        ufs_file_base* result =
            new syscall_file(cfg.path, mode, cfg.queue, disk_allocator_id,
                             cfg.device_id);
        result->lock();

        // if marked as device but file is not -> throw!
        if (cfg.raw_device && !result->is_device())
        {
            delete result;
            STXXL_THROW(io_error, "Disk " << cfg.path << " was expected to be "
                        "a raw block device, but it is a normal file!");
        }

        // if is raw_device -> get size and remove some flags.
        if (result->is_device())
        {
            cfg.raw_device = true;
            cfg.size = result->size();
            cfg.autogrow = cfg.delete_on_exit = cfg.unlink_on_open = false;
        }

        if (cfg.unlink_on_open)
            result->unlink();

        return result;
    }
    else if (cfg.io_impl == "memory")
    {
        memory_file* result = new memory_file(cfg.queue, disk_allocator_id, cfg.device_id);
        result->lock();
        return result;
    }
#if STXXL_HAVE_LINUXAIO_FILE
    // linuxaio can have the desired queue length, specified as queue_length=?
    else if (cfg.io_impl == "linuxaio")
    {
        // linuxaio_queue is a singleton.
        cfg.queue = file::DEFAULT_LINUXAIO_QUEUE;

        ufs_file_base* result =
            new linuxaio_file(cfg.path, mode, cfg.queue, disk_allocator_id,
                              cfg.device_id, cfg.queue_length);

        result->lock();

        // if marked as device but file is not -> throw!
        if (cfg.raw_device && !result->is_device())
        {
            delete result;
            STXXL_THROW(io_error, "Disk " << cfg.path << " was expected to be "
                        "a raw block device, but it is a normal file!");
        }

        // if is raw_device -> get size and remove some flags.
        if (result->is_device())
        {
            cfg.raw_device = true;
            cfg.size = result->size();
            cfg.autogrow = cfg.delete_on_exit = cfg.unlink_on_open = false;
        }

        if (cfg.unlink_on_open)
            result->unlink();

        return result;
    }
#endif
#if STXXL_HAVE_MMAP_FILE
    else if (cfg.io_impl == "mmap")
    {
        ufs_file_base* result =
            new mmap_file(cfg.path, mode, cfg.queue, disk_allocator_id,
                          cfg.device_id);
        result->lock();

        if (cfg.unlink_on_open)
            result->unlink();

        return result;
    }
#endif
#if STXXL_HAVE_WINCALL_FILE
    else if (cfg.io_impl == "wincall")
    {
        wfs_file_base* result =
            new wincall_file(cfg.path, mode, cfg.queue, disk_allocator_id,
                             cfg.device_id);
        result->lock();
        return result;
    }
#endif

    STXXL_THROW(std::runtime_error,
                "Unsupported disk I/O implementation '" << cfg.io_impl << "'.");
}

} // namespace io
} // namespace thrill

/******************************************************************************/
