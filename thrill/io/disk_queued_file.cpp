/*******************************************************************************
 * thrill/io/disk_queued_file.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/disk_queued_file.hpp>
#include <thrill/io/disk_queues.hpp>
#include <thrill/io/file.hpp>
#include <thrill/io/request.hpp>
#include <thrill/io/request_interface.hpp>
#include <thrill/io/serving_request.hpp>

namespace thrill {
namespace io {

request_ptr disk_queued_file::aread(
    void* buffer,
    offset_type pos,
    size_type bytes,
    const completion_handler& on_cmpl) {
    request_ptr req(new serving_request(on_cmpl, this, buffer, pos, bytes,
                                        request::READ));

    disk_queues::get_instance()->add_request(req, get_queue_id());

    return req;
}

request_ptr disk_queued_file::awrite(
    void* buffer,
    offset_type pos,
    size_type bytes,
    const completion_handler& on_cmpl) {
    request_ptr req(new serving_request(on_cmpl, this, buffer, pos, bytes,
                                        request::WRITE));

    disk_queues::get_instance()->add_request(req, get_queue_id());

    return req;
}

} // namespace io
} // namespace thrill

/******************************************************************************/
