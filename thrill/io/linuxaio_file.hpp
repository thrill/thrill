/*******************************************************************************
 * thrill/io/linuxaio_file.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2011 Johannes Singler <singler@kit.edu>
 * Copyright (C) 2014 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_LINUXAIO_FILE_HEADER
#define THRILL_IO_LINUXAIO_FILE_HEADER

#include <thrill/common/config.hpp>

#if THRILL_HAVE_LINUXAIO_FILE

#include <thrill/io/disk_queued_file.hpp>
#include <thrill/io/linuxaio_queue.hpp>
#include <thrill/io/ufs_file_base.hpp>

#include <string>

namespace thrill {
namespace io {

class LinuxaioQueue;

//! \addtogroup io_layer_fileimpl
//! \{

//! Implementation of \c file based on the Linux kernel interface for
//! asynchronous I/O
class LinuxaioFile final : public UfsFileBase, public DiskQueuedFile
{
    friend class LinuxaioRequest;

private:
    int desired_queue_length_;

public:
    //! Constructs file object
    //! \param filename path of file
    //! \param mode open mode, see \c FileBase::OpenMode
    //! \param queue_id disk queue identifier
    //! \param allocator_id linked disk_allocator
    //! \param device_id physical device identifier
    //! \param desired_queue_length queue length requested from kernel
    LinuxaioFile(
        const std::string& filename, int mode,
        int queue_id = DEFAULT_LINUXAIO_QUEUE,
        int allocator_id = NO_ALLOCATOR,
        unsigned int device_id = DEFAULT_DEVICE_ID,
        int desired_queue_length = 0)
        : FileBase(device_id),
          UfsFileBase(filename, mode),
          DiskQueuedFile(queue_id, allocator_id),
          desired_queue_length_(desired_queue_length)
    { }

    void serve(void* buffer, offset_type offset, size_type bytes,
               Request::ReadOrWriteType type) final;
    RequestPtr aread(void* buffer, offset_type offset, size_type bytes,
                     const CompletionHandler& on_cmpl = CompletionHandler()) final;
    RequestPtr awrite(void* buffer, offset_type offset, size_type bytes,
                      const CompletionHandler& on_cmpl = CompletionHandler()) final;
    const char * io_type() const final;

    int desired_queue_length() const {
        return desired_queue_length_;
    }
};

//! \}

} // namespace io
} // namespace thrill

#endif // #if THRILL_HAVE_LINUXAIO_FILE

#endif // !THRILL_IO_LINUXAIO_FILE_HEADER

/******************************************************************************/
