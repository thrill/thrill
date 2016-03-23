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

#include <thrill/io/file_base.hpp>

#include <mutex>
#include <string>

namespace thrill {
namespace io {

//! \addtogroup io_layer_fileimpl
//! \{

//! Base for UNIX file system implementations.
class UfsFileBase : public virtual FileBase
{
protected:
    std::mutex fd_mutex_; // sequentialize function calls involving file_des
    int file_des_;        // file descriptor
    int mode_;            // open mode
    const std::string path_;
    bool is_device_;      //!< is special device node
    UfsFileBase(const std::string& filename, int mode);
    void _after_open();
    offset_type _size();
    void _set_size(offset_type newsize);
    void close();

public:
    ~UfsFileBase();
    offset_type size() final;
    void set_size(offset_type newsize) final;
    void lock() final;
    const char * io_type() const override;
    void close_remove() final;
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
