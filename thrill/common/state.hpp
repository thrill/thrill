/*******************************************************************************
 * thrill/common/state.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_STATE_HEADER
#define THRILL_COMMON_STATE_HEADER

#include <condition_variable>
#include <mutex>

namespace thrill {
namespace common {

template <typename ValueType = int>
class state
{
    using value_type = ValueType;

    //! mutex for condition variable
    std::mutex m_mutex;

    //! condition variable
    std::condition_variable m_cond;

    //! current state
    value_type m_state;

public:
    explicit state(const value_type& s)
        : m_state(s)
    { }

    //! non-copyable: delete copy-constructor
    state(const state&) = delete;
    //! non-copyable: delete assignment operator
    state& operator = (const state&) = delete;

    void set_to(const value_type& new_state) {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_state = new_state;
        lock.unlock();
        m_cond.notify_all();
    }

    void wait_for(const value_type& needed_state) {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (needed_state != m_state)
            m_cond.wait(lock);
    }

    value_type operator () () {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_state;
    }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_STATE_HEADER

/******************************************************************************/
