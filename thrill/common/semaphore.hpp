/*******************************************************************************
 * thrill/common/semaphore.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2013-2016 Timo Bingmann <tb@panthema.net>
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

class Semaphore
{
    //! value of the semaphore
    int value_;

    //! mutex for condition variable
    std::mutex mutex_;

    //! condition variable
    std::condition_variable cv_;

public:
    //! construct semaphore
    explicit Semaphore(int init_value = 0)
        : value_(init_value) { }

    //! non-copyable: delete copy-constructor
    Semaphore(const Semaphore&) = delete;
    //! non-copyable: delete assignment operator
    Semaphore& operator = (const Semaphore&) = delete;

    //! function increments the semaphore and signals any threads that are
    //! blocked waiting a change in the semaphore
    int notify() {
        std::unique_lock<std::mutex> lock(mutex_);
        int res = ++value_;
        cv_.notify_one();
        return res;
    }
    //! function increments the semaphore and signals any threads that are
    //! blocked waiting a change in the semaphore
    int notify(int delta) {
        std::unique_lock<std::mutex> lock(mutex_);
        int res = (value_ += delta);
        cv_.notify_all();
        return res;
    }
    //! function decrements the semaphore and blocks if the semaphore is <= 0
    //! until another thread signals a change
    int wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (value_ <= 0)
            cv_.wait(lock);

        return --value_;
    }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_SEMAPHORE_HEADER

/******************************************************************************/
