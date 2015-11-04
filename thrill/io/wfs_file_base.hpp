/*******************************************************************************
 * thrill/io/wfs_file_base.hpp
 *
 * Windows file system file base
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2005 Roman Dementiev <dementiev@ira.uka.de>
 * Copyright (C) 2008, 2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009, 2010 Johannes Singler <singler@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_WFS_FILE_BASE_HEADER
#define THRILL_IO_WFS_FILE_BASE_HEADER

#include <thrill/common/config.hpp>

#if STXXL_WINDOWS

#include <thrill/io/file.h>
#include <thrill/io/request.h>

namespace thrill {
namespace io {

//! \addtogroup fileimpl
//! \{

//! Base for Windows file system implementations.
class wfs_file_base : public virtual file
{
protected:
    using HANDLE = void*;

    mutex fd_mutex;        // sequentialize function calls involving file_des
    HANDLE file_des;       // file descriptor
    int mode_;             // open mode
    const std::string filename;
    offset_type bytes_per_sector;
    bool locked;
    wfs_file_base(const std::string& filename, int mode);
    offset_type _size();
    void close();

public:
    ~wfs_file_base();
    offset_type size();
    void set_size(offset_type newsize);
    void lock();
    const char * io_type() const;
    void close_remove();
};

//! \}

} // namespace io
} // namespace thrill

#endif // STXXL_WINDOWS

#endif // !THRILL_IO_WFS_FILE_BASE_HEADER

/******************************************************************************/
