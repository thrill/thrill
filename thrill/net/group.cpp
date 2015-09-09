/*******************************************************************************
 * thrill/net/group.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/net/group.hpp>
#include <thrill/net/mock/group.hpp>
#include <thrill/net/tcp/group.hpp>

#include <thread>

namespace thrill {
namespace net {

void RunGroupTest(
    size_t num_hosts,
    const std::function<void(Group*)>& thread_function) {
    // construct mock network mesh and run threads
    ExecuteGroupThreads(
        mock::Group::ConstructLocalMesh(num_hosts),
        thread_function);
#if 0
    // construct local tcp network mesh and run threads
    ExecuteGroupThreads(
        tcp::Group::ConstructLocalMesh(num_hosts),
        thread_function);
#endif
}

} // namespace net
} // namespace thrill

/******************************************************************************/
