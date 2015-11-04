/*******************************************************************************
 * thrill/io/disk_queued_file.hpp
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
#ifndef THRILL_IO_DISK_QUEUED_FILE_HEADER
#define THRILL_IO_DISK_QUEUED_FILE_HEADER

#include <thrill/io/file.hpp>
#include <thrill/io/request.hpp>

namespace thrill {
namespace io {

//! \addtogroup fileimpl
//! \{

class completion_handler;

//! Implementation of some file methods based on serving_request.
class disk_queued_file : public virtual file
{
    int m_queue_id, m_allocator_id;

public:
    disk_queued_file(int queue_id, int allocator_id)
        : m_queue_id(queue_id), m_allocator_id(allocator_id)
    { }

    request_ptr aread(
        void* buffer,
        offset_type pos,
        size_type bytes,
        const completion_handler& on_cmpl = completion_handler()) override;

    request_ptr awrite(
        void* buffer,
        offset_type pos,
        size_type bytes,
        const completion_handler& on_cmpl = completion_handler()) override;

    virtual int get_queue_id() const override {
        return m_queue_id;
    }

    virtual int get_allocator_id() const override {
        return m_allocator_id;
    }
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_DISK_QUEUED_FILE_HEADER

/******************************************************************************/
