/*******************************************************************************
 * thrill/io/mmap_file.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_MMAP_FILE_HEADER
#define THRILL_IO_MMAP_FILE_HEADER

#include <thrill/common/config.hpp>

#if THRILL_HAVE_MMAP_FILE

#include <thrill/io/disk_queued_file.hpp>
#include <thrill/io/ufs_file_base.hpp>

#include <string>

namespace thrill {
namespace io {

//! \addtogroup io_layer_fileimpl
//! \{

//! Implementation of memory mapped access file.
class MmapFile final : public UfsFileBase, public DiskQueuedFile
{
public:
    //! Constructs file object.
    //! \param filename path of file
    //! \param mode open mode, see \c FileBase::OpenMode
    //! \param queue_id disk queue identifier
    //! \param allocator_id linked disk_allocator
    //! \param device_id physical device identifier
    MmapFile(
        const std::string& filename,
        int mode,
        int queue_id = DEFAULT_QUEUE,
        int allocator_id = NO_ALLOCATOR,
        unsigned int device_id = DEFAULT_DEVICE_ID)
        : FileBase(device_id),
          UfsFileBase(filename, mode),
          DiskQueuedFile(queue_id, allocator_id)
    { }
    void serve(void* buffer, offset_type offset, size_type bytes,
               Request::ReadOrWriteType type) final;
    const char * io_type() const final;
};

//! \}

} // namespace io
} // namespace thrill

#endif // #if THRILL_HAVE_MMAP_FILE

#endif // !THRILL_IO_MMAP_FILE_HEADER

/******************************************************************************/
