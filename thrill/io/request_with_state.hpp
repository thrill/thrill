/*******************************************************************************
 * thrill/io/request_with_state.hpp
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

#pragma once
#ifndef THRILL_IO_REQUEST_WITH_STATE_HEADER
#define THRILL_IO_REQUEST_WITH_STATE_HEADER

#include <thrill/common/state.hpp>
#include <thrill/io/request.hpp>
#include <thrill/io/request_with_waiters.hpp>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

//! Request with completion state.
class request_with_state : public request_with_waiters
{
protected:
    //! states of request
    //! OP - operating, DONE - request served, READY2DIE - can be destroyed
    enum request_state { OP = 0, DONE = 1, READY2DIE = 2 };

    common::state<request_state> m_state;

protected:
    request_with_state(
        const completion_handler& on_cmpl,
        file* f,
        void* buf,
        offset_type off,
        size_type b,
        request_type t)
        : request_with_waiters(on_cmpl, f, buf, off, b, t),
          m_state(OP)
    { }

public:
    virtual ~request_with_state();
    void wait(bool measure_time = true);
    bool poll();
    bool cancel();

protected:
    void completed(bool canceled);
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_REQUEST_WITH_STATE_HEADER

/******************************************************************************/
