/*******************************************************************************
 * thrill/io/request_with_state.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002, 2005, 2008 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/state.hpp>
#include <thrill/io/disk_queues.hpp>
#include <thrill/io/file.hpp>
#include <thrill/io/iostats.hpp>
#include <thrill/io/request.hpp>
#include <thrill/io/request_with_state.hpp>

#include <cassert>

namespace thrill {
namespace io {

request_with_state::~request_with_state() {
    LOG << "request_with_state::~(), ref_cnt: " << reference_count();

    assert(m_state() == DONE || m_state() == READY2DIE);

    // if(m_state() != DONE && m_state()!= READY2DIE )
    // STXXL_ERRMSG("WARNING: serious stxxl inconsistency: Request is being deleted while I/O not finished. "<<
    //              "Please submit a bug report.");

    // m_state.wait_for (READY2DIE); // does not make sense ?
}

void request_with_state::wait(bool measure_time) {
    LOG << "request_with_state::wait()";

    stats::scoped_wait_timer wait_timer(type_ == READ ? stats::WAIT_OP_READ : stats::WAIT_OP_WRITE, measure_time);

    m_state.wait_for(READY2DIE);

    check_errors();
}

bool request_with_state::cancel() {
    LOG << "request_with_state::cancel() " << file_ << " " << buffer_ << " " << offset_;

    if (file_)
    {
        request_ptr rp(this);
        if (disk_queues::get_instance()->cancel_request(rp, file_->get_queue_id()))
        {
            m_state.set_to(DONE);
            notify_waiters();
            file_->delete_request_ref();
            file_ = 0;
            m_state.set_to(READY2DIE);
            return true;
        }
    }
    return false;
}

bool request_with_state::poll() {
    const request_state s = m_state();

    check_errors();

    return s == DONE || s == READY2DIE;
}

void request_with_state::completed(bool canceled) {
    LOG << "request_with_state::completed()";
    m_state.set_to(DONE);
    if (!canceled)
        on_complete_(this);
    notify_waiters();
    file_->delete_request_ref();
    file_ = 0;
    m_state.set_to(READY2DIE);
}

} // namespace io
} // namespace thrill

/******************************************************************************/
