/*******************************************************************************
 * thrill/io/linuxaio_request.cpp
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

#include <thrill/io/linuxaio_request.hpp>

#if STXXL_HAVE_LINUXAIO_FILE

#include "error_handling.hpp"
#include <thrill/io/disk_queues.hpp>

#include <sys/syscall.h>
#include <unistd.h>

namespace thrill {
namespace io {

void linuxaio_request::completed(bool posted, bool canceled) {
    LOG << "linuxaio_request[" << this << "] completed("
        << posted << "," << canceled << ")";

    if (!canceled)
    {
        if (type_ == READ)
            stats::get_instance()->read_finished();
        else
            stats::get_instance()->write_finished();
    }
    else if (posted)
    {
        if (type_ == READ)
            stats::get_instance()->read_canceled(bytes_);
        else
            stats::get_instance()->write_canceled(bytes_);
    }
    request_with_state::completed(canceled);
}

void linuxaio_request::fill_control_block() {
    linuxaio_file* af = dynamic_cast<linuxaio_file*>(file_);

    memset(&cb, 0, sizeof(cb));
    // indirection, so the I/O system retains a counting_ptr reference
    cb.aio_data = reinterpret_cast<__u64>(new request_ptr(this));
    cb.aio_fildes = af->file_des;
    cb.aio_lio_opcode = (type_ == READ) ? IOCB_CMD_PREAD : IOCB_CMD_PWRITE;
    cb.aio_reqprio = 0;
    cb.aio_buf = static_cast<__u64>((unsigned long)(buffer_));
    cb.aio_nbytes = bytes_;
    cb.aio_offset = offset_;
}

//! Submits an I/O request to the OS
//! \returns false if submission fails
bool linuxaio_request::post() {
    LOG << "linuxaio_request[" << this << "] post()";

    fill_control_block();
    iocb* cb_pointer = &cb;
    // io_submit might considerable time, so we have to remember the current
    // time before the call.
    double now = timestamp();
    linuxaio_queue* queue = dynamic_cast<linuxaio_queue*>(
        disk_queues::get_instance()->get_queue(file_->get_queue_id())
        );
    long success = syscall(SYS_io_submit, queue->get_io_context(), 1, &cb_pointer);
    if (success == 1)
    {
        if (type_ == READ)
            stats::get_instance()->read_started(bytes_, now);
        else
            stats::get_instance()->write_started(bytes_, now);
    }
    else if (success == -1 && errno != EAGAIN)
        STXXL_THROW_ERRNO(io_error, "linuxaio_request::post"
                          " io_submit()");

    return success == 1;
}

//! Cancel the request
//!
//! Routine is called by user, as part of the request interface.
bool linuxaio_request::cancel() {
    LOG1 << "linuxaio_request[" << this << "] cancel()";

    if (!file_) return false;

    request_ptr req(this);
    linuxaio_queue* queue = dynamic_cast<linuxaio_queue*>(
        disk_queues::get_instance()->get_queue(file_->get_queue_id())
        );
    return queue->cancel_request(req);
}

//! Cancel already posted request
bool linuxaio_request::cancel_aio() {
    LOG1 << "linuxaio_request[" << this << "] cancel_aio()";

    if (!file_) return false;

    io_event event;
    linuxaio_queue* queue = dynamic_cast<linuxaio_queue*>(
        disk_queues::get_instance()->get_queue(file_->get_queue_id())
        );
    long result = syscall(SYS_io_cancel, queue->get_io_context(), &cb, &event);
    if (result == 0)    //successfully canceled
        queue->handle_events(&event, 1, true);
    return result == 0;
}

} // namespace io
} // namespace thrill

#endif // #if STXXL_HAVE_LINUXAIO_FILE

/******************************************************************************/
