/*******************************************************************************
 * c7a/net/manager.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/net/construction.hpp>
#include <c7a/net/manager.hpp>

#include <vector>

namespace c7a {
namespace net {

void Manager::Initialize(size_t my_rank,
                         const std::vector<Endpoint>& endpoints) {
    my_rank_ = my_rank;
    Construction(*this).Initialize(my_rank_, endpoints);
}

//! Construct a mock network, consisting of node_count compute
//! nodes. Delivers this number of net::Manager objects, which are
//! internally connected.
std::vector<Manager> Manager::ConstructLocalMesh(size_t node_count) {

    // construct list of uninitialized net::Manager objects.
    std::vector<Manager> nmlist(node_count);

    for (size_t n = 0; n < node_count; ++n) {
        nmlist[n].my_rank_ = n;
    }

    // construct three full mesh connection cliques, deliver net::Groups.
    for (size_t g = 0; g < kGroupCount; ++g) {
        std::vector<Group> group = Group::ConstructLocalMesh(node_count);

        // distribute net::Group objects to managers
        for (size_t n = 0; n < node_count; ++n) {
            nmlist[n].groups_[g] = std::move(group[n]);
        }
    }

    return std::move(nmlist);
}

} // namespace net
} // namespace c7a

/******************************************************************************/
