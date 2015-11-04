/*******************************************************************************
 * thrill/io/memory_file.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2014 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_MEMORY_FILE_HEADER
#define THRILL_IO_MEMORY_FILE_HEADER

#include <thrill/io/disk_queued_file.hpp>
#include <thrill/io/request.hpp>

#include <mutex>

namespace thrill {
namespace io {

//! \addtogroup fileimpl
//! \{

//! Implementation of file based on new[] and memcpy.
class memory_file : public disk_queued_file
{
    //! pointer to memory area of "file"
    char* m_ptr;

    //! size of memory area
    offset_type m_size;

    //! sequentialize function calls
    std::mutex m_mutex;

public:
    //! constructs file object.
    memory_file(
        int queue_id = DEFAULT_QUEUE,
        int allocator_id = NO_ALLOCATOR,
        unsigned int device_id = DEFAULT_DEVICE_ID)
        : file(device_id),
          disk_queued_file(queue_id, allocator_id),
          m_ptr(nullptr), m_size(0)
    { }
    void serve(void* buffer, offset_type offset, size_type bytes,
               request::request_type type);
    ~memory_file();
    offset_type size();
    void set_size(offset_type newsize);
    void lock();
    void discard(offset_type offset, offset_type size);
    const char * io_type() const;
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_MEMORY_FILE_HEADER

/******************************************************************************/
