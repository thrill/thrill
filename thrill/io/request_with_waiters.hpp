/*******************************************************************************
 * thrill/io/request_with_waiters.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_REQUEST_WITH_WAITERS_HEADER
#define THRILL_IO_REQUEST_WITH_WAITERS_HEADER

#include <mutex>
#include <set>

#include <thrill/common/onoff_switch.hpp>
#include <thrill/io/request.hpp>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

//! Request that is aware of threads waiting for it to complete.
class request_with_waiters : public request
{
    std::mutex m_waiters_mutex;
    std::set<common::onoff_switch*> m_waiters;

protected:
    bool add_waiter(common::onoff_switch* sw);
    void delete_waiter(common::onoff_switch* sw);
    void notify_waiters();

    //! returns number of waiters
    size_t num_waiters();

public:
    request_with_waiters(
        const completion_handler& on_cmpl,
        io::file* f,
        void* buf,
        offset_type off,
        size_type b,
        ReadOrWriteType t)
        : request(on_cmpl, f, buf, off, b, t)
    { }
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_REQUEST_WITH_WAITERS_HEADER

/******************************************************************************/
