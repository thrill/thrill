/*******************************************************************************
 * thrill/io/request_with_waiters.cpp
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

#include <thrill/common/onoff_switch.hpp>
#include <thrill/io/request_with_waiters.hpp>

#include <algorithm>
#include <functional>
#include <mutex>

namespace thrill {
namespace io {

bool request_with_waiters::add_waiter(common::onoff_switch* sw) {
    // this lock needs to be obtained before poll(), otherwise a race
    // condition might occur: the state might change and notify_waiters()
    // could be called between poll() and insert() resulting in waiter sw
    // never being notified
    std::unique_lock<std::mutex> lock(m_waiters_mutex);

    if (poll())                     // request already finished
    {
        return true;
    }

    m_waiters.insert(sw);

    return false;
}

void request_with_waiters::delete_waiter(common::onoff_switch* sw) {
    std::unique_lock<std::mutex> lock(m_waiters_mutex);
    m_waiters.erase(sw);
}

void request_with_waiters::notify_waiters() {
    std::unique_lock<std::mutex> lock(m_waiters_mutex);
    std::for_each(m_waiters.begin(),
                  m_waiters.end(),
                  std::mem_fun(&common::onoff_switch::on));
}

size_t request_with_waiters::num_waiters() {
    std::unique_lock<std::mutex> lock(m_waiters_mutex);
    return m_waiters.size();
}

} // namespace io
} // namespace thrill

/******************************************************************************/
