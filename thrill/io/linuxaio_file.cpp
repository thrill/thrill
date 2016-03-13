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

#if THRILL_HAVE_LINUXAIO_FILE

#include <thrill/io/disk_queues.hpp>
#include <thrill/io/linuxaio_request.hpp>
#include <thrill/mem/pool.hpp>

namespace thrill {
namespace io {

RequestPtr LinuxaioFile::aread(
    void* buffer, offset_type pos, size_type bytes,
    const CompletionHandler& on_cmpl) {

    RequestPtr req(mem::g_pool.make<LinuxaioRequest>(
                       on_cmpl, this, buffer, pos, bytes, Request::READ));

    DiskQueues::get_instance()->add_request(req, get_queue_id());

    return req;
}

RequestPtr LinuxaioFile::awrite(
    void* buffer, offset_type pos, size_type bytes,
    const CompletionHandler& on_cmpl) {

    RequestPtr req(mem::g_pool.make<LinuxaioRequest>(
                       on_cmpl, this, buffer, pos, bytes, Request::WRITE));

    DiskQueues::get_instance()->add_request(req, get_queue_id());

    return req;
}

void LinuxaioFile::serve(void* buffer, offset_type offset, size_type bytes,
                         Request::ReadOrWriteType type) {
    // req need not be an linuxaio_request
    if (type == Request::READ)
        aread(buffer, offset, bytes)->wait();
    else
        awrite(buffer, offset, bytes)->wait();
}

const char* LinuxaioFile::io_type() const {
    return "linuxaio";
}

} // namespace io
} // namespace thrill

#endif // #if THRILL_HAVE_LINUXAIO_FILE

/******************************************************************************/
