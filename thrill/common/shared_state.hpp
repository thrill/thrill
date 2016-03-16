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

template <typename ValueType = size_t>
class SharedState
{
    using value_type = ValueType;

    //! mutex for condition variable
    std::mutex mutex_;

    //! condition variable
    std::condition_variable cv_;

    //! current state
    value_type state_;

public:
    explicit SharedState(const value_type& s)
        : state_(s)
    { }

    //! non-copyable: delete copy-constructor
    SharedState(const SharedState&) = delete;
    //! non-copyable: delete assignment operator
    SharedState& operator = (const SharedState&) = delete;

    void set_to(const value_type& new_state) {
        std::unique_lock<std::mutex> lock(mutex_);
        state_ = new_state;
        lock.unlock();
        cv_.notify_all();
    }

    void wait_for(const value_type& needed_state) {
        std::unique_lock<std::mutex> lock(mutex_);
        while (needed_state != state_)
            cv_.wait(lock);
    }

    value_type operator () () {
        std::unique_lock<std::mutex> lock(mutex_);
        return state_;
    }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_STATE_HEADER

/******************************************************************************/
