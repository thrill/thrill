/*******************************************************************************
 * thrill/io/ufs_file_base.hpp
 *
 * UNIX file system file base
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_UFS_FILE_BASE_HEADER
#define THRILL_IO_UFS_FILE_BASE_HEADER

#include <thrill/io/file.hpp>

#include <mutex>
#include <string>

namespace thrill {
namespace io {

//! \addtogroup fileimpl
//! \{

//! Base for UNIX file system implementations.
class ufs_file_base : public virtual file
{
protected:
    std::mutex fd_mutex;   // sequentialize function calls involving file_des
    int file_des;          // file descriptor
    int m_mode;            // open mode
    const std::string filename;
    bool m_is_device;      //!< is special device node
    ufs_file_base(const std::string& filename, int mode);
    void _after_open();
    offset_type _size();
    void _set_size(offset_type newsize);
    void close();

public:
    ~ufs_file_base();
    offset_type size();
    void set_size(offset_type newsize);
    void lock();
    const char * io_type() const;
    void close_remove();
    //! unlink file without closing it.
    void unlink();
    //! return true if file is special device node
    bool is_device() const;
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_UFS_FILE_BASE_HEADER

/******************************************************************************/
