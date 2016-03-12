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

#include <thrill/io/file_base.hpp>
#include <thrill/io/request.hpp>

namespace thrill {
namespace io {

//! \addtogroup fileimpl
//! \{

//! Implementation of some file methods based on serving_request.
class DiskQueuedFile : public virtual FileBase
{
    int queue_id_, allocator_id_;

public:
    DiskQueuedFile(int queue_id, int allocator_id)
        : queue_id_(queue_id), allocator_id_(allocator_id)
    { }

    RequestPtr aread(
        void* buffer, offset_type pos, size_type bytes,
        const CompletionHandler& on_cmpl = CompletionHandler()) override;

    RequestPtr awrite(
        void* buffer, offset_type pos, size_type bytes,
        const CompletionHandler& on_cmpl = CompletionHandler()) override;

    int get_queue_id() const override {
        return queue_id_;
    }

    int get_allocator_id() const override {
        return allocator_id_;
    }
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_DISK_QUEUED_FILE_HEADER

/******************************************************************************/
