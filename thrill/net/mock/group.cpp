/*******************************************************************************
 * thrill/net/mock/group.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/net/mock/group.hpp>

#include <vector>

namespace thrill {
namespace net {
namespace mock {

mem::mm_unique_ptr<net::Dispatcher> Group::ConstructDispatcher(
    mem::Manager& mem_manager) const {
    // construct mock::Dispatcher
    return mem::mm_unique_ptr<net::Dispatcher>(
        mem::mm_new<Dispatcher>(mem_manager, mem_manager),
        mem::Deleter<net::Dispatcher>(mem_manager));
}

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

void Connection::SyncSend(const void* data, size_t size, Flags /* flags */) {
    group_->Send(peer_, net::Buffer(data, size));
}

void Connection::InboundMsg(net::Buffer&& msg) {
    std::unique_lock<std::mutex> lock(mutex_);
    inbound_.emplace_back(std::move(msg));
    cv_.notify_all();
    for (Dispatcher* d : watcher_)
        d->Notify(this);
}

/******************************************************************************/

void Dispatcher::DispatchOne(const std::chrono::milliseconds& timeout) {

    Connection* c = nullptr;
    if (!notify_.pop_for(c, timeout)) {
        sLOG << "DispatchOne timeout";
        return;
    }

    if (c == nullptr) {
        sLOG << "DispatchOne interrupt";
        return;
    }

    sLOG << "DispatchOne run";

    std::unique_lock<std::mutex> d_lock(mutex_);

    Map::iterator it = map_.find(c);
    if (it == map_.end()) {
        sLOG << "DispatchOne expired connection?";
        return;
    }

    Watch& w = it->second;
    assert(w.active);

    std::unique_lock<std::mutex> c_lock(c->mutex_);

    // check for readability
    if (w.read_cb.size() && c->inbound_.size()) {

        while (c->inbound_.size() && w.read_cb.size()) {
            c_lock.unlock();
            d_lock.unlock();

            bool ret = true;
            try {
                ret = w.read_cb.front()();
            }
            catch (std::exception& e) {
                LOG1 << "Dispatcher: exception " << typeid(e).name()
                     << "in read callback.";
                LOG1 << "  what(): " << e.what();
                throw;
            }

            d_lock.lock();
            c_lock.lock();

            if (ret) break;
            w.read_cb.pop_front();
        }

        if (w.read_cb.size() == 0 && w.write_cb.size() == 0) {
            // if all callbacks are done, listen no longer.
            c->watcher_.erase(this);
            map_.erase(it);
            return;
        }
    }

    // "check" for writable. virtual sockets are always writable.
    if (w.write_cb.size()) {

        while (w.write_cb.size()) {
            c_lock.unlock();
            d_lock.unlock();

            bool ret = true;
            try {
                ret = w.write_cb.front()();
            }
            catch (std::exception& e) {
                LOG1 << "Dispatcher: exception " << typeid(e).name()
                     << "in write callback.";
                LOG1 << "  what(): " << e.what();
                throw;
            }

            d_lock.lock();
            c_lock.lock();

            if (ret) break;
            w.write_cb.pop_front();
        }

        if (w.read_cb.size() == 0 && w.write_cb.size() == 0) {
            // if all callbacks are done, listen no longer.
            c->watcher_.erase(this);
            map_.erase(it);
            return;
        }
    }
}

} // namespace mock
} // namespace net
} // namespace thrill

/******************************************************************************/
