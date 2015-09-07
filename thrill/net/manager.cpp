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
#include <thrill/net/tcp/connection.hpp>
#include <thrill/net/tcp/construction.hpp>
#include <thrill/net/tcp/group.hpp>
#include <thrill/net/tcp/select_dispatcher.hpp>

#include <deque>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

Manager::Manager(size_t my_rank,
                 const std::vector<std::string>& endpoints)
    : my_rank_(my_rank) {
    try {
        tcp::Construction(*this).Initialize(my_rank_, endpoints);
    }
    catch (std::exception& e) {
        LOG1 << "Exception: " << e.what();
        throw;
    }
}

Manager::Manager(size_t my_rank,
                 std::array<Group, kGroupCount>&& groups)
    : my_rank_(my_rank),
      groups_(std::move(groups)) { }

//! Construct a mock network, consisting of host_count compute
//! hosts. Delivers this number of net::Manager objects, which are
//! internally connected.
std::vector<std::unique_ptr<Manager> >
Manager::ConstructLocalMesh(size_t host_count) {

    // construct three full mesh connection cliques, deliver net::Groups.
    std::array<std::vector<Group>, kGroupCount> group;

    for (size_t g = 0; g < kGroupCount; ++g) {
        group[g] = Group::ConstructLocalMesh(host_count);
    }

    // construct list of uninitialized net::Manager objects.
    std::vector<std::unique_ptr<Manager> > nmlist(host_count);

    for (size_t h = 0; h < host_count; ++h) {
        std::array<Group, kGroupCount> host_group = {
            { std::move(group[0][h]),
              std::move(group[1][h]),
              std::move(group[2][h]) },
        };

        nmlist[h] = std::make_unique<Manager>(h, std::move(host_group));
    }

    return nmlist;
}

//! \}

} // namespace net
} // namespace thrill

/******************************************************************************/
