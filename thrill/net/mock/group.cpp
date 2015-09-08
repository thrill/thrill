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

/******************************************************************************/

void Connection::SyncSend(const void* data, size_t size, int /* flags */) {
    group_->Send(peer_, net::Buffer(data, size));
}

void Connection::InboundMsg(net::Buffer&& msg) {
    std::unique_lock<std::mutex> lock(mutex_);
    inbound_.emplace_back(std::move(msg));
    cv_.notify_all();
    for (Dispatcher* d : watcher_)
        d->Notify(this);
}

} // namespace mock
} // namespace net
} // namespace thrill

/******************************************************************************/
