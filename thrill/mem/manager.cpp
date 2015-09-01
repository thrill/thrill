/*******************************************************************************
 * thrill/mem/manager.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/mem/manager.hpp>

#include <cstdio>

namespace thrill {
namespace mem {

Manager::~Manager() {
    printf("mem::Manager() name=%s alloc_count_=%lu peak_=%lu total_=%lu\n",
           name_, alloc_count_.load(), peak_.load(), total_.load());
}

Manager g_bypass_manager(nullptr, "Bypass");

} // namespace mem
} // namespace thrill

/******************************************************************************/
