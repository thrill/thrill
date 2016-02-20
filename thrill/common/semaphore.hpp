/*******************************************************************************
 * thrill/common/semaphore.hpp
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
#ifndef THRILL_COMMON_SEMAPHORE_HEADER
#define THRILL_COMMON_SEMAPHORE_HEADER

#include <condition_variable>
#include <mutex>

namespace thrill {
namespace common {

class semaphore
{
    //! value of the semaphore
    int value_;

    //! mutex for condition variable
    std::mutex mutex_;

    //! condition variable
    std::condition_variable cv_;

public:
    //! construct semaphore
    explicit semaphore(int init_value = 1)
        : value_(init_value)
    { }

    //! non-copyable: delete copy-constructor
    semaphore(const semaphore&) = delete;
    //! non-copyable: delete assignment operator
    semaphore& operator = (const semaphore&) = delete;

    //! function increments the semaphore and signals any threads that are
    //! blocked waiting a change in the semaphore
    int operator ++ (int) { // NOLINT
        std::unique_lock<std::mutex> lock(mutex_);
        int res = ++value_;
        cv_.notify_one();
        return res;
    }
    //! function decrements the semaphore and blocks if the semaphore is <= 0
    //! until another thread signals a change
    int operator -- (int) { // NOLINT
        std::unique_lock<std::mutex> lock(mutex_);
        while (value_ <= 0)
            cv_.wait(lock);

        return --value_;
    }
    //! function does NOT block but simply decrements the semaphore should not
    //! be used instead of down -- only for programs where multiple threads
    //! must up on a semaphore before another thread can go down, i.e., allows
    //! programmer to set the semaphore to a negative value prior to using it
    //! for synchronization.
    int decrement() {
        std::unique_lock<std::mutex> lock(mutex_);
        return --value_;
    }
#if 0
    //! function returns the value of the semaphore at the time the
    //! critical section is accessed.  obviously the value is not guaranteed
    //! after the function unlocks the critical section.
    int get_value() {
        scoped_mutex_lock lock(mutex_);
        return value_;
    }
#endif
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_SEMAPHORE_HEADER

/******************************************************************************/
