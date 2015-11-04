/*******************************************************************************
 * thrill/io/mmap_file.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/mmap_file.hpp>

#if STXXL_HAVE_MMAP_FILE

#include "ufs_platform.h"
#include <stxxl/bits/common/error_handling.h>
#include <sys/mman.h>
#include <thrill/io/iostats.h>

namespace thrill {
namespace io {

void mmap_file::serve(void* buffer, offset_type offset, size_type bytes,
                      request::request_type type) {
    scoped_mutex_lock fd_lock(fd_mutex);

    // assert(offset + bytes <= _size());

    stats::scoped_read_write_timer read_write_timer(bytes, type == request::WRITE);

    int prot = (type == request::READ) ? PROT_READ : PROT_WRITE;
    void* mem = mmap(nullptr, bytes, prot, MAP_SHARED, file_des, offset);
    // void *mem = mmap (buffer, bytes, prot , MAP_SHARED|MAP_FIXED , file_des, offset);
    // STXXL_MSG("Mmaped to "<<mem<<" , buffer suggested at "<<buffer);
    if (mem == MAP_FAILED)
    {
        STXXL_THROW_ERRNO(io_error,
                          " mmap() failed." <<
                          " path=" << filename <<
                          " bytes=" << bytes <<
                          " Page size: " << sysconf(_SC_PAGESIZE) <<
                          " offset modulo page size " << (offset % sysconf(_SC_PAGESIZE)));
    }
    else if (mem == 0)
    {
        STXXL_THROW_ERRNO(io_error, "mmap() returned nullptr");
    }
    else
    {
        if (type == request::READ)
        {
            memcpy(buffer, mem, bytes);
        }
        else
        {
            memcpy(mem, buffer, bytes);
        }
        STXXL_THROW_ERRNO_NE_0(munmap(mem, bytes), io_error,
                               "munmap() failed");
    }
}

const char* mmap_file::io_type() const {
    return "mmap";
}

} // namespace io
} // namespace thrill

#endif  // #if STXXL_HAVE_MMAP_FILE

/******************************************************************************/
