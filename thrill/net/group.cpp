/*******************************************************************************
 * thrill/net/group.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/net/group.hpp>
#include <thrill/net/mock/group.hpp>

#if THRILL_HAVE_NET_TCP
#include <thrill/net/tcp/group.hpp>
#endif

namespace thrill {
namespace net {

void RunLoopbackGroupTest(
    size_t num_hosts,
    const std::function<void(Group*)>& thread_function) {
#if THRILL_HAVE_NET_TCP
    // construct local tcp network mesh and run threads
    ExecuteGroupThreads(
        tcp::Group::ConstructLoopbackMesh(num_hosts),
        thread_function);
#else
    // construct mock network mesh and run threads
    ExecuteGroupThreads(
        mock::Group::ConstructLoopbackMesh(num_hosts),
        thread_function);
#endif
}

} // namespace net
} // namespace thrill

/******************************************************************************/
