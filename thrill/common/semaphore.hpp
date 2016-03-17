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
public:
    //! construct semaphore
    explicit Semaphore(size_t init_value = 0)
        : value_(init_value) { }

    //! non-copyable: delete copy-constructor
    Semaphore(const Semaphore&) = delete;
    //! non-copyable: delete assignment operator
    Semaphore& operator = (const Semaphore&) = delete;
    //! move-constructor: just move the value
    Semaphore(Semaphore&& s) : value_(s.value_) { }

    //! function increments the semaphore and signals any threads that are
    //! blocked waiting a change in the semaphore
    size_t signal() {
        std::unique_lock<std::mutex> lock(mutex_);
        size_t res = ++value_;
        cv_.notify_one();
        return res;
    }
    //! function increments the semaphore and signals any threads that are
    //! blocked waiting a change in the semaphore
    size_t signal(size_t delta) {
        std::unique_lock<std::mutex> lock(mutex_);
        size_t res = (value_ += delta);
        cv_.notify_all();
        return res;
    }
    //! function decrements the semaphore and blocks if the semaphore is <= 0
    //! until another thread signals a change
    size_t wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (value_ <= 0)
            cv_.wait(lock);
        return --value_;
    }

private:
    //! value of the semaphore
    size_t value_;

    //! mutex for condition variable
    std::mutex mutex_;

    //! condition variable
    std::condition_variable cv_;
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_SEMAPHORE_HEADER

/******************************************************************************/
