/*******************************************************************************
 * thrill/net/mock/group.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/net/mock/group.hpp>

namespace thrill {
namespace net {
namespace mock {

std::vector<std::unique_ptr<Group> >
Group::ConstructLocalMesh(size_t num_hosts) {

    std::vector<std::unique_ptr<Group> > groups(num_hosts);

    // first construct all the Group objects.
    for (size_t i = 0; i < groups.size(); ++i) {
        groups[i] = std::make_unique<Group>(i, num_hosts);
    }

    // then interconnect them
    for (size_t i = 0; i < groups.size(); ++i) {
        for (size_t j = 0; j < groups.size(); ++j) {
            groups[i]->peers_[j] = groups[j].get();
        }
    }

    return groups;
}

} // namespace mock
} // namespace net
} // namespace thrill

/******************************************************************************/
