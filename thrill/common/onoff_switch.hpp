/*******************************************************************************
 * thrill/common/onoff_switch.hpp
 *
 * Kind of binary semaphore: initially OFF, then multiple waiters can attach
 * to the switch, which get notified one-by-one when switched ON.
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_ONOFF_SWITCH_HEADER
#define THRILL_COMMON_ONOFF_SWITCH_HEADER

#include <condition_variable>
#include <mutex>

namespace thrill {
namespace common {

class onoff_switch
{
    //! mutex for condition variable
    std::mutex mutex_;

    //! condition variable
    std::condition_variable cv_;

    //! the switch's state
    bool on_;

public:
    //! construct switch
    explicit onoff_switch(bool flag = false)
        : on_(flag)
    { }

    //! non-copyable: delete copy-constructor
    onoff_switch(const onoff_switch&) = delete;
    //! non-copyable: delete assignment operator
    onoff_switch& operator = (const onoff_switch&) = delete;

    //! turn switch ON and notify one waiter
    void on() {
        std::unique_lock<std::mutex> lock(mutex_);
        on_ = true;
        lock.unlock();
        cv_.notify_one();
    }
    //! turn switch OFF and notify one waiter
    void off() {
        std::unique_lock<std::mutex> lock(mutex_);
        on_ = false;
        lock.unlock();
        cv_.notify_one();
    }
    //! wait for switch to turn ON
    void wait_for_on() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!on_)
            cv_.wait(lock);
    }
    //! wait for switch to turn OFF
    void wait_for_off() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (on_)
            cv_.wait(lock);
    }
    //! return true if switch is ON
    bool is_on() {
        std::unique_lock<std::mutex> lock(mutex_);
        return on_;
    }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_ONOFF_SWITCH_HEADER

/******************************************************************************/
