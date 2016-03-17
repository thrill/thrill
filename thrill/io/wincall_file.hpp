/*******************************************************************************
 * thrill/io/wincall_file.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2005-2006 Roman Dementiev <dementiev@ira.uka.de>
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009-2010 Johannes Singler <singler@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_WINCALL_FILE_HEADER
#define THRILL_IO_WINCALL_FILE_HEADER

#include <thrill/common/config.hpp>

#ifndef THRILL_HAVE_WINCALL_FILE
#if THRILL_WINDOWS
 #define THRILL_HAVE_WINCALL_FILE 1
#else
 #define THRILL_HAVE_WINCALL_FILE 0
#endif
#endif

#if THRILL_HAVE_WINCALL_FILE

#include <thrill/io/disk_queued_file.hpp>
#include <thrill/io/wfs_file_base.hpp>

#include <string>

namespace thrill {
namespace io {

//! \addtogroup fileimpl
//! \{

//! Implementation of file based on Windows native I/O calls.
class WincallFile final : public WfsFileBase, public DiskQueuedFile
{
public:
    //! Constructs file object.
    //! \param filename path of file
    //! \param mode open mode, see \c FileBase::OpenMode
    //! \param queue_id disk queue identifier
    //! \param allocator_id linked disk_allocator
    //! \param device_id physical device identifier
    WincallFile(
        const std::string& filename,
        int mode,
        int queue_id = DEFAULT_QUEUE,
        int allocator_id = NO_ALLOCATOR,
        unsigned int device_id = DEFAULT_DEVICE_ID)
        : FileBase(device_id),
          WfsFileBase(filename, mode),
          DiskQueuedFile(queue_id, allocator_id)
    { }
    void serve(void* buffer, offset_type offset, size_type bytes,
               Request::ReadOrWriteType type) final;
    const char * io_type() const final;
};

//! \}

} // namespace io
} // namespace thrill

#endif // #if THRILL_HAVE_WINCALL_FILE

#endif // !THRILL_IO_WINCALL_FILE_HEADER

/******************************************************************************/
