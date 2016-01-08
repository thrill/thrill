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

#if THRILL_HAVE_MMAP_FILE

#include <thrill/io/error_handling.hpp>
#include <thrill/io/iostats.hpp>
#include <thrill/io/ufs_platform.hpp>

#include <sys/mman.h>

namespace thrill {
namespace io {

void MmapFile::serve(void* buffer, offset_type offset, size_type bytes,
                     Request::ReadOrWriteType type) {
    std::unique_lock<std::mutex> fd_lock(fd_mutex_);

    // assert(offset + bytes <= _size());

    Stats::scoped_read_write_timer read_write_timer(bytes, type == Request::WRITE);

    int prot = (type == Request::READ) ? PROT_READ : PROT_WRITE;
    void* mem = mmap(nullptr, bytes, prot, MAP_SHARED, file_des, offset);
    // void *mem = mmap (buffer, bytes, prot , MAP_SHARED|MAP_FIXED , file_des, offset);
    // THRILL_MSG("Mmaped to "<<mem<<" , buffer suggested at "<<buffer);
    if (mem == MAP_FAILED)
    {
        THRILL_THROW_ERRNO(IoError,
                           " mmap() failed." <<
                           " path=" << filename <<
                           " bytes=" << bytes <<
                           " Page size: " << sysconf(_SC_PAGESIZE) <<
                           " offset modulo page size " << (offset % sysconf(_SC_PAGESIZE)));
    }
    else if (mem == 0)
    {
        THRILL_THROW_ERRNO(IoError, "mmap() returned nullptr");
    }
    else
    {
        if (type == Request::READ)
        {
            memcpy(buffer, mem, bytes);
        }
        else
        {
            memcpy(mem, buffer, bytes);
        }
        THRILL_THROW_ERRNO_NE_0(munmap(mem, bytes), IoError,
                                "munmap() failed");
    }
}

const char* MmapFile::io_type() const {
    return "mmap";
}

} // namespace io
} // namespace thrill

#endif  // #if THRILL_HAVE_MMAP_FILE

/******************************************************************************/
