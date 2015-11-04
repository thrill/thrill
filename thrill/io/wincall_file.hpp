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

#ifndef STXXL_HAVE_WINCALL_FILE
#if STXXL_WINDOWS
 #define STXXL_HAVE_WINCALL_FILE 1
#else
 #define STXXL_HAVE_WINCALL_FILE 0
#endif
#endif

#if STXXL_HAVE_WINCALL_FILE

#include <thrill/io/disk_queued_file.h>
#include <thrill/io/wfs_file_base.h>

namespace thrill {
namespace io {

//! \addtogroup fileimpl
//! \{

//! Implementation of file based on Windows native I/O calls.
class wincall_file final : public wfs_file_base, public disk_queued_file
{
public:
    //! Constructs file object.
    //! \param filename path of file
    //! \param mode open mode, see \c stxxl::file::open_modes
    //! \param queue_id disk queue identifier
    //! \param allocator_id linked disk_allocator
    //! \param device_id physical device identifier
    wincall_file(
        const std::string& filename,
        int mode,
        int queue_id = DEFAULT_QUEUE,
        int allocator_id = NO_ALLOCATOR,
        unsigned int device_id = DEFAULT_DEVICE_ID)
        : file(device_id),
          wfs_file_base(filename, mode),
          disk_queued_file(queue_id, allocator_id)
    { }
    void serve(void* buffer, offset_type offset, size_type bytes,
               request::request_type type) final;
    const char * io_type() const final;
};

//! \}

} // namespace io
} // namespace thrill

#endif // #if STXXL_HAVE_WINCALL_FILE

#endif // !THRILL_IO_WINCALL_FILE_HEADER

/******************************************************************************/
