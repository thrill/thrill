/*******************************************************************************
 * thrill/net/mpi/group.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/net/mpi/group.hpp>

#include <vector>

namespace thrill {
namespace net {
namespace mpi {

std::mutex g_mutex;

mem::mm_unique_ptr<net::Dispatcher> Group::ConstructDispatcher(
    mem::Manager& mem_manager) const {
    // construct mpi::Dispatcher
    return mem::mm_unique_ptr<net::Dispatcher>(
        mem::mm_new<Dispatcher>(mem_manager,
                                mem_manager, group_tag_, num_hosts()),
        mem::Deleter<net::Dispatcher>(mem_manager));
}

} // namespace mpi
} // namespace net
} // namespace thrill

/******************************************************************************/
