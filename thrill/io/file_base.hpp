/*******************************************************************************
 * thrill/io/file_base.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2008, 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2013-2014 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_FILE_BASE_HEADER
#define THRILL_IO_FILE_BASE_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/counting_ptr.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/io/bid.hpp>
#include <thrill/io/request.hpp>

#if __linux__
 #define THRILL_CHECK_BLOCK_ALIGNING
#endif

#include <cassert>
#include <ostream>
#include <string>

namespace thrill {
namespace io {

//! \addtogroup io_layer
//! \{

//! \defgroup io_layer_fileimpl File I/O Implementations
//! Implementations of \ref FileBase for various file access methods and
//! operating systems.
//! \{

//! Defines interface of file.
//!
//! It is a base class for different implementations that might
//! base on various file systems or even remote storage interfaces
class FileBase : public common::ReferenceCount
{
public:
    static constexpr bool debug = false;

    //! the offset of a request, also the size of the file
    using offset_type = Request::offset_type;
    //! the size of a request
    using size_type = Request::size_type;

    //! non-copyable: delete copy-constructor
    FileBase(const FileBase&) = delete;
    //! non-copyable: delete assignment operator
    FileBase& operator = (const FileBase&) = delete;
    //! move-constructor: default
    FileBase(FileBase&&) = default;
    //! move-assignment operator: default
    FileBase& operator = (FileBase&&) = default;

    //! Definition of acceptable file open modes.  Various open modes in a file
    //! system must be converted to this set of acceptable modes.
    enum OpenMode
    {
        //! only reading of the file is allowed
        RDONLY = 1,
        //! only writing of the file is allowed
        WRONLY = 2,
        //! read and write of the file are allowed
        RDWR = 4,
        //! in case file does not exist no error occurs and file is newly
        //! created
        CREAT = 8,
        //! I/Os proceed bypassing file system buffers, i.e. unbuffered I/O.
        //! Tries to open with appropriate flags, if fails print warning and
        //! open normally.
        DIRECT = 16,
        //! once file is opened its length becomes zero
        TRUNC = 32,
        //! open the file with O_SYNC | O_DSYNC | O_RSYNC flags set
        SYNC = 64,
        //! do not acquire an exclusive lock by default
        NO_LOCK = 128,
        //! implies DIRECT, fail if opening with DIRECT flag does not work.
        REQUIRE_DIRECT = 256
    };

    static constexpr int DEFAULT_QUEUE = -1;
    static constexpr int DEFAULT_LINUXAIO_QUEUE = -2;
    static constexpr int NO_ALLOCATOR = -1;
    static constexpr unsigned int DEFAULT_DEVICE_ID = (unsigned int)(-1);

    //! Construct a new file, usually called by a subclass.
    explicit FileBase(unsigned int device_id = DEFAULT_DEVICE_ID)
        : device_id_(device_id) { }

    //! Schedules an asynchronous read request to the file.
    //! \param buffer pointer to memory buffer to read into
    //! \param offset file position to start read from
    //! \param bytes number of bytes to transfer
    //! \param on_cmpl I/O completion handler
    //! \return \c request_ptr request object, which can be used to track the
    //! status of the operation

    virtual RequestPtr aread(
        void* buffer, offset_type offset, size_type bytes,
        const CompletionHandler& on_cmpl = CompletionHandler()) = 0;

    //! Schedules an asynchronous write request to the file.
    //! \param buffer pointer to memory buffer to write from
    //! \param offset starting file position to write
    //! \param bytes number of bytes to transfer
    //! \param on_cmpl I/O completion handler
    //! \return \c request_ptr request object, which can be used to track the
    //! status of the operation
    virtual RequestPtr awrite(
        void* buffer, offset_type offset, size_type bytes,
        const CompletionHandler& on_cmpl = CompletionHandler()) = 0;

    virtual void serve(void* buffer, offset_type offset, size_type bytes,
                       Request::ReadOrWriteType type) = 0;

    //! Changes the size of the file.
    //! \param newsize new file size
    virtual void set_size(offset_type newsize) = 0;

    //! Returns size of the file.
    //! \return file size in bytes
    virtual offset_type size() = 0;

    //! Returns the identifier of the file's queue number.
    //! \remark Files allocated on the same physical device usually share the
    //! same queue, unless there is a common queue (e.g. with linuxaio).
    virtual int get_queue_id() const = 0;

    //! Returns the file's disk allocator number
    virtual int get_allocator_id() const = 0;

    //! Locks file for reading and writing (acquires a lock in the file system).
    virtual void lock() = 0;

    //! Discard a region of the file (mark it unused).
    //! Some specialized file types may need to know freed regions
    virtual void discard(offset_type offset, offset_type size) {
        common::UNUSED(offset);
        common::UNUSED(size);
    }

    //! close and remove file
    virtual void close_remove() { }

    virtual ~FileBase() { }

    //! Identifies the type of I/O implementation.
    //! \return pointer to null terminated string of characters, containing the
    //! name of I/O implementation
    virtual const char * io_type() const = 0;

protected:
    //! Flag whether read/write operations require alignment
    bool need_alignment_ = false;

    //! The file's physical device id (e.g. used for prefetching sequence
    //! calculation)
    unsigned int device_id_;

public:
    //! Returns need_alignment_
    bool need_alignment() const { return need_alignment_; }

    //! Returns the file's physical device id
    unsigned int get_device_id() const {
        return device_id_;
    }

public:
    //! \name Static Functions for Platform Abstraction
    //! \{

    //! unlink path from filesystem
    static int unlink(const char* path);

    //! truncate a path to given length. Use this only if you dont have a
    //! fileio-specific object, which provides truncate().
    static int truncate(const char* path, size_t length);

    //! \}
};

using FileBasePtr = common::CountingPtr<FileBase>;

// implementation here due to forward declaration of file.
template <size_t Size>
bool BID<Size>::is_managed() const {
    return storage->get_allocator_id() != FileBase::NO_ALLOCATOR;
}

inline
bool BID<0>::is_managed() const {
    return storage->get_allocator_id() != FileBase::NO_ALLOCATOR;
}

//! \}

//! \defgroup io_layer_req I/O Requests and Queues
//! Encapsulation of an I/O request, queues for requests and threads to process
//! them.
//! \{
//! \}

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_FILE_BASE_HEADER

/******************************************************************************/
