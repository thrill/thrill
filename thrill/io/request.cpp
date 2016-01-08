/*******************************************************************************
 * thrill/io/request.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/disk_queues.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/io/iostats.hpp>
#include <thrill/io/request.hpp>
#include <thrill/mem/aligned_alloc.hpp>

#include <ostream>

namespace thrill {
namespace io {

/******************************************************************************/

Request::Request(
    const CompletionHandler& on_complete,
    io::FileBase* file,
    void* buffer,
    offset_type offset,
    size_type bytes,
    ReadOrWriteType type)
    : on_complete_(on_complete),
      file_(file),
      buffer_(buffer),
      offset_(offset),
      bytes_(bytes),
      type_(type) {
    LOG << "request::(...), ref_cnt=" << reference_count();
    file_->add_request_ref();
}

Request::~Request() {
    LOG << "request::~request(), ref_cnt=" << reference_count();
    assert(state_() == DONE || state_() == READY2DIE);
}

void Request::check_alignment() const {
    if (offset_ % THRILL_DEFAULT_ALIGN != 0)
        LOG1 << "Offset is not aligned: modulo "
             << THRILL_DEFAULT_ALIGN << " = " << offset_ % THRILL_DEFAULT_ALIGN;

    if (bytes_ % THRILL_DEFAULT_ALIGN != 0)
        LOG1 << "Size is not a multiple of "
             << THRILL_DEFAULT_ALIGN << ", = " << bytes_ % THRILL_DEFAULT_ALIGN;

    if (uintptr_t(buffer_) % THRILL_DEFAULT_ALIGN != 0)
        LOG1 << "Buffer is not aligned: modulo "
             << THRILL_DEFAULT_ALIGN << " = " << size_t(buffer_) % THRILL_DEFAULT_ALIGN
             << " (" << buffer_ << ")";
}

void Request::check_nref_failed(bool after) {
    LOG1 << "WARNING: serious error, reference to the request is lost "
         << (after ? "after" : "before") << " serve()"
         << " nref=" << reference_count()
         << " this=" << this
         << " offset=" << offset_
         << " buffer=" << buffer_
         << " bytes=" << bytes_
         << " type=" << ((type_ == READ) ? "READ" : "WRITE")
         << " file=" << file_
         << " iotype=" << file_->io_type();
}

const char* Request::io_type() const {
    return file_->io_type();
}

std::ostream& Request::print(std::ostream& out) const {
    out << "File object address: " << static_cast<void*>(file_);
    out << " Buffer address: " << static_cast<void*>(buffer_);
    out << " File offset: " << offset_;
    out << " Transfer size: " << bytes_ << " bytes";
    out << " Type of transfer: " << ((type_ == READ) ? "READ" : "WRITE");
    return out;
}

/******************************************************************************/
// Waiters

bool Request::add_waiter(common::onoff_switch* sw) {
    // this lock needs to be obtained before poll(), otherwise a race
    // condition might occur: the state might change and notify_waiters()
    // could be called between poll() and insert() resulting in waiter sw
    // never being notified
    std::unique_lock<std::mutex> lock(waiters_mutex_);

    if (poll()) {
        // request already finished
        return true;
    }

    waiters_.insert(sw);

    return false;
}

void Request::delete_waiter(common::onoff_switch* sw) {
    std::unique_lock<std::mutex> lock(waiters_mutex_);
    waiters_.erase(sw);
}

void Request::notify_waiters() {
    std::unique_lock<std::mutex> lock(waiters_mutex_);
    std::for_each(waiters_.begin(),
                  waiters_.end(),
                  std::mem_fun(&common::onoff_switch::on));
}

size_t Request::num_waiters() {
    std::unique_lock<std::mutex> lock(waiters_mutex_);
    return waiters_.size();
}

/******************************************************************************/
// Request Completion State

void Request::wait(bool measure_time) {
    LOG << "request::wait()";

    Stats::scoped_wait_timer wait_timer(
        type_ == READ ? Stats::WAIT_OP_READ : Stats::WAIT_OP_WRITE,
        measure_time);

    state_.wait_for(READY2DIE);

    check_error();
}

bool Request::cancel() {
    LOG << "request::cancel() " << file_ << " " << buffer_ << " " << offset_;

    if (file_) {
        RequestPtr rp(this);
        if (DiskQueues::get_instance()->cancel_request(rp, file_->get_queue_id()))
        {
            state_.set_to(DONE);
            // user callback
            if (on_complete_)
                on_complete_(this, false);
            notify_waiters();
            file_->delete_request_ref();
            file_ = 0;
            state_.set_to(READY2DIE);
            return true;
        }
    }
    return false;
}

bool Request::poll() {
    const State s = state_();

    check_error();

    return s == DONE || s == READY2DIE;
}

void Request::completed(bool canceled) {
    LOG << "request::completed()";
    // change state
    state_.set_to(DONE);
    // user callback
    if (on_complete_)
        on_complete_(this, !canceled);
    // notify waiters
    notify_waiters();
    file_->delete_request_ref();
    file_ = 0;
    state_.set_to(READY2DIE);
}

} // namespace io
} // namespace thrill

/******************************************************************************/
