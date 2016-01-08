/*******************************************************************************
 * thrill/common/singleton.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2004 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008-2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009, 2010 Johannes Singler <singler@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_SINGLETON_HEADER
#define THRILL_COMMON_SINGLETON_HEADER

#include <sys/time.h>

#include <mutex>

namespace thrill {
namespace common {

template <typename Instance, bool destroy_on_exit = true>
class Singleton
{
    using instance_type = Instance;
    using instance_pointer = instance_type *;
    using volatile_instance_pointer = volatile instance_pointer;

    static volatile_instance_pointer instance_;

    static instance_pointer create_instance();
    static void destroy_instance();

public:
    Singleton() = default;

    //! non-copyable: delete copy-constructor
    Singleton(const Singleton&) = delete;
    //! non-copyable: delete assignment operator
    Singleton& operator = (const Singleton&) = delete;
    //! move-constructor: default
    Singleton(Singleton&&) = default;
    //! move-assignment operator: default
    Singleton& operator = (Singleton&&) = default;

    inline static instance_pointer get_instance() {
        if (!instance_)
            return create_instance();

        return instance_;
    }
};

template <typename Instance, bool destroy_on_exit>
typename Singleton<Instance, destroy_on_exit>::instance_pointer
Singleton<Instance, destroy_on_exit>::create_instance() {
    static std::mutex create_mutex;
    std::unique_lock<std::mutex> lock(create_mutex);
    if (!instance_) {
        instance_ = new instance_type();
        if (destroy_on_exit)
            atexit(destroy_instance);
    }
    return instance_;
}

template <typename Instance, bool destroy_on_exit>
void Singleton<Instance, destroy_on_exit>::destroy_instance() {
    instance_pointer inst = instance_;
    // instance = nullptr;
    instance_ = reinterpret_cast<instance_pointer>(size_t(-1));     // bomb if used again
    delete inst;
}

template <typename Instance, bool destroy_on_exit>
typename Singleton<Instance, destroy_on_exit>::volatile_instance_pointer
Singleton<Instance, destroy_on_exit>::instance_ = nullptr;

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_SINGLETON_HEADER

/******************************************************************************/
