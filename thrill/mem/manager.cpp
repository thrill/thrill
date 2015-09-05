/*******************************************************************************
 * thrill/mem/manager.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/mem/manager.hpp>

#include <cstdio>

namespace thrill {
namespace mem {

Manager::~Manager() {
    // You can not use the logger here, because there is maybe no LoggerAllocator
    // any more
    if (debug) {
        printf("mem::Manager() name=%s alloc_count_=%zu peak_=%zu total_=%zu\n",
               name_, alloc_count_.load(), peak_.load(), total_.load());
    }
}

Manager g_bypass_manager(nullptr, "Bypass");

} // namespace mem
} // namespace thrill

/******************************************************************************/
