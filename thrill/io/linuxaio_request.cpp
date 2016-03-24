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

#if THRILL_HAVE_LINUXAIO_FILE

#include <thrill/io/disk_queues.hpp>
#include <thrill/io/error_handling.hpp>
#include <thrill/io/iostats.hpp>

#include <sys/syscall.h>
#include <unistd.h>

namespace thrill {
namespace io {

void LinuxaioRequest::completed(bool posted, bool canceled) {
    LOG << "LinuxaioRequest[" << this << "] completed("
        << posted << "," << canceled << ")";

    if (!canceled)
    {
        if (type_ == READ)
            Stats::GetInstance()->read_finished();
        else
            Stats::GetInstance()->write_finished();
    }
    else if (posted)
    {
        if (type_ == READ)
            Stats::GetInstance()->read_canceled(bytes_);
        else
            Stats::GetInstance()->write_canceled(bytes_);
    }
    Request::completed(canceled);
}

void LinuxaioRequest::fill_control_block() {
    LinuxaioFile* af = dynamic_cast<LinuxaioFile*>(file_.get());

    memset(&cb_, 0, sizeof(cb_));
    // indirection, so the I/O system retains a counting_ptr reference
    cb_.aio_data = reinterpret_cast<__u64>(new RequestPtr(this));
    cb_.aio_fildes = af->file_des_;
    cb_.aio_lio_opcode = (type_ == READ) ? IOCB_CMD_PREAD : IOCB_CMD_PWRITE;
    cb_.aio_reqprio = 0;
    cb_.aio_buf = static_cast<__u64>((unsigned long)(buffer_));
    cb_.aio_nbytes = bytes_;
    cb_.aio_offset = offset_;
}

//! Submits an I/O request to the OS
//! \returns false if submission fails
bool LinuxaioRequest::post() {
    LOG << "LinuxaioRequest[" << this << "] post()";

    fill_control_block();
    iocb* cb_pointer = &cb_;
    // io_submit might considerable time, so we have to remember the current
    // time before the call.
    double now = timestamp();
    LinuxaioQueue* queue = dynamic_cast<LinuxaioQueue*>(
        DiskQueues::GetInstance()->GetQueue(file_->get_queue_id()));
    long success = syscall(SYS_io_submit, queue->io_context(), 1, &cb_pointer);
    if (success == 1)
    {
        if (type_ == READ)
            Stats::GetInstance()->read_started(bytes_, now);
        else
            Stats::GetInstance()->write_started(bytes_, now);
    }
    else if (success == -1 && errno != EAGAIN)
        THRILL_THROW_ERRNO(IoError, "LinuxaioRequest::post"
                           " io_submit()");

    return success == 1;
}

//! Cancel the request
//!
//! Routine is called by user, as part of the request interface.
bool LinuxaioRequest::cancel() {
    LOG << "LinuxaioRequest[" << this << "] cancel()";

    if (!file_) return false;

    RequestPtr req(this);
    LinuxaioQueue* queue = dynamic_cast<LinuxaioQueue*>(
        DiskQueues::GetInstance()->GetQueue(file_->get_queue_id()));
    return queue->CancelRequest(req);
}

//! Cancel already posted request
bool LinuxaioRequest::cancel_aio() {
    LOG << "LinuxaioRequest[" << this << "] cancel_aio()";

    if (!file_) return false;

    io_event event;
    LinuxaioQueue* queue = dynamic_cast<LinuxaioQueue*>(
        DiskQueues::GetInstance()->GetQueue(file_->get_queue_id()));
    long result = syscall(SYS_io_cancel, queue->io_context(), &cb_, &event);
    if (result == 0)    //successfully canceled
        queue->HandleEvents(&event, 1, true);
    return result == 0;
}

} // namespace io
} // namespace thrill

#endif // #if THRILL_HAVE_LINUXAIO_FILE

/******************************************************************************/
