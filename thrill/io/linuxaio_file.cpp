/*******************************************************************************
 * thrill/io/linuxaio_file.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2011 Johannes Singler <singler@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/linuxaio_file.hpp>

#if STXXL_HAVE_LINUXAIO_FILE

#include <thrill/io/disk_queues.hpp>
#include <thrill/io/linuxaio_request.hpp>

namespace thrill {
namespace io {

request_ptr linuxaio_file::aread(
    void* buffer,
    offset_type pos,
    size_type bytes,
    const completion_handler& on_cmpl) {
    request_ptr req(new linuxaio_request(on_cmpl, this, buffer, pos, bytes, request::READ));

    disk_queues::get_instance()->add_request(req, get_queue_id());

    return req;
}

request_ptr linuxaio_file::awrite(
    void* buffer,
    offset_type pos,
    size_type bytes,
    const completion_handler& on_cmpl) {
    request_ptr req(new linuxaio_request(on_cmpl, this, buffer, pos, bytes, request::WRITE));

    disk_queues::get_instance()->add_request(req, get_queue_id());

    return req;
}

void linuxaio_file::serve(void* buffer, offset_type offset, size_type bytes,
                          request::ReadOrWriteType type) {
    // req need not be an linuxaio_request
    if (type == request::READ)
        aread(buffer, offset, bytes)->wait();
    else
        awrite(buffer, offset, bytes)->wait();
}

const char* linuxaio_file::io_type() const {
    return "linuxaio";
}

} // namespace io
} // namespace thrill

#endif // #if STXXL_HAVE_LINUXAIO_FILE

/******************************************************************************/
