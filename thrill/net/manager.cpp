/*******************************************************************************
 * thrill/net/manager.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/net/manager.hpp>
#include <thrill/net/tcp/construct.hpp>

#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace thrill {
namespace net {

Manager::Manager(size_t my_rank,
                 const std::vector<std::string>& endpoints)
    : my_rank_(my_rank) {
    try {
        std::array<std::unique_ptr<tcp::Group>, kGroupCount> tcp_groups;
        tcp::Construct(my_rank_, endpoints, tcp_groups.data(), tcp_groups.size());

        // move into object, upcast to net::Group.
        std::move(tcp_groups.begin(), tcp_groups.end(), groups_.begin());
    }
    catch (std::exception& e) {
        LOG1 << "Exception: " << e.what();
        throw;
    }
}

} // namespace net
} // namespace thrill

/******************************************************************************/
