/*******************************************************************************
 * thrill/net/mock/group.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/concurrent_bounded_queue.hpp>
#include <thrill/net/mock/group.hpp>

#include <tlx/die.hpp>
#include <tlx/string/hexdump.hpp>

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace thrill {
namespace net {
namespace mock {

/******************************************************************************/
// mock::Connection

class Connection::Data
{
public:
    //! Mutex to lock access to inbound message queue
    std::mutex mutex_;

    //! Condition variable to wake up threads synchronously blocking on
    //! messages.
    std::condition_variable cv_;

    //! Set of watching dispatchers.
    std::set<Dispatcher*> watcher_;

    //! type of message queue
    using DataQueue = std::deque<net::Buffer>;

    //! inbound message queue the virtual network peer
    DataQueue inbound_;
};

void Connection::Initialize(Group* group, size_t peer) {
    d_ = std::make_unique<Data>();
    group_ = group;
    peer_ = peer;
    is_loopback_ = true;
}

void Connection::InboundMsg(net::Buffer&& msg) {
    std::unique_lock<std::mutex> lock(d_->mutex_);
    d_->inbound_.emplace_back(std::move(msg));
    d_->cv_.notify_all();
    for (Dispatcher* d : d_->watcher_)
        d->Notify(this);
}

std::string Connection::ToString() const {
    return "peer: " + std::to_string(peer_);
}

std::ostream& Connection::OutputOstream(std::ostream& os) const {
    return os << "[mock::Connection"
              << " group=" << group_
              << " peer=" << peer_
              << "]";
}
void Connection::SyncSend(const void* data, size_t size, Flags /* flags */) {
    // set errno : success (unconditionally)
    errno = 0;
    group_->Send(peer_, net::Buffer(data, size));
    tx_bytes_ += size;
}

ssize_t Connection::SendOne(const void* data, size_t size, Flags flags) {
    SyncSend(data, size, flags);
    return size;
}

net::Buffer Connection::RecvNext() {
    std::unique_lock<std::mutex> lock(d_->mutex_);
    while (d_->inbound_.empty())
        d_->cv_.wait(lock);
    net::Buffer msg = std::move(d_->inbound_.front());
    d_->inbound_.pop_front();

    // set errno : success (other syscalls may have failed)
    errno = 0;
    rx_bytes_ += msg.size();

    return msg;
}

void Connection::SyncRecv(void* out_data, size_t size) {
    net::Buffer msg = RecvNext();
    die_unequal(msg.size(), size);
    char* out_cdata = reinterpret_cast<char*>(out_data);
    std::copy(msg.begin(), msg.end(), out_cdata);
}

ssize_t Connection::RecvOne(void* out_data, size_t size) {
    SyncRecv(out_data, size);
    return size;
}

void Connection::SyncSendRecv(const void* send_data, size_t send_size,
                              void* recv_data, size_t recv_size) {
    SyncSend(send_data, send_size, NoFlags);
    SyncRecv(recv_data, recv_size);
}

void Connection::SyncRecvSend(const void* send_data, size_t send_size,
                              void* recv_data, size_t recv_size) {
    SyncRecv(recv_data, recv_size);
    SyncSend(send_data, send_size, NoFlags);
}

/******************************************************************************/
// mock::Group

Group::Group(size_t my_rank, size_t group_size)
    : net::Group(my_rank) {
    peers_.resize(group_size);
    // create virtual connections, due to complications with non-movable
    // mutexes, use a plain new.
    conns_ = new Connection[group_size];
    for (size_t i = 0; i < group_size; ++i)
        conns_[i].Initialize(this, i);
}

Group::~Group() {
    delete[] conns_;
}

size_t Group::num_hosts() const {
    return peers_.size();
}

net::Connection& Group::connection(size_t peer) {
    assert(peer < peers_.size());
    return conns_[peer];
}

void Group::Close() { }

std::unique_ptr<net::Dispatcher> Group::ConstructDispatcher() const {
    // construct mock::Dispatcher
    return std::make_unique<Dispatcher>();
}

std::vector<std::unique_ptr<Group> >
Group::ConstructLoopbackMesh(size_t num_hosts) {

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

std::string Group::MaybeHexdump(const void* data, size_t size) {
    if (debug_data)
        return tlx::hexdump(data, size);
    else
        return "[data]";
}

void Group::Send(size_t tgt, net::Buffer&& msg) {
    assert(tgt < peers_.size());

    if (debug) {
        sLOG << "Sending" << my_rank_ << "->" << tgt
             << "msg" << MaybeHexdump(msg.data(), msg.size());
    }

    peers_[tgt]->conns_[my_rank_].InboundMsg(std::move(msg));
}

/******************************************************************************/

class Dispatcher::Data
{
public:
    //! Mutex to lock access to watch lists
    std::mutex mutex_;

    //! Notification queue for Dispatch
    common::ConcurrentBoundedQueue<Connection*> notify_;

    using Map = std::map<Connection*, Watch>;

    //! map from Connection to its watch list
    Map map_;
};

class Dispatcher::Watch
{
public:
    //! boolean check whether Watch is registered at Connection
    bool active = false;
    //! queue of callbacks for fd.
    std::deque<Callback, mem::GPoolAllocator<Callback> > read_cb, write_cb;
    //! only one exception callback for the fd.
    Callback except_cb;
};

Dispatcher::Dispatcher()
    : net::Dispatcher(),
      d_(std::make_unique<Data>())
{ }

Dispatcher::~Dispatcher()
{ }

//! Register a buffered read callback and a default exception callback.
void Dispatcher::AddRead(net::Connection& _c, const Callback& read_cb) {
    assert(dynamic_cast<Connection*>(&_c));
    Connection& c = static_cast<Connection&>(_c);

    std::unique_lock<std::mutex> d_lock(d_->mutex_);
    Watch& w = GetWatch(&c);
    w.read_cb.emplace_back(read_cb);
    if (!w.active) {
        std::unique_lock<std::mutex> c_lock(c.d_->mutex_);
        c.d_->watcher_.insert(this);
        w.active = true;
        // if already have a packet, issue notification.
        if (c.d_->inbound_.size())
            Notify(&c);
    }
}

void Dispatcher::AddWrite(net::Connection& _c, const Callback& write_cb) {
    assert(dynamic_cast<Connection*>(&_c));
    Connection& c = static_cast<Connection&>(_c);

    std::unique_lock<std::mutex> d_lock(d_->mutex_);
    Watch& w = GetWatch(&c);
    w.write_cb.emplace_back(write_cb);
    if (!w.active) {
        std::unique_lock<std::mutex> c_lock(c.d_->mutex_);
        c.d_->watcher_.insert(this);
        w.active = true;
    }
    // our virtual sockets are always writable: issue notification.
    Notify(&c);
}

void Dispatcher::Cancel(net::Connection&) {
    abort();
}

void Dispatcher::Notify(Connection* c) {
    d_->notify_.emplace(c);
}

void Dispatcher::Interrupt() {
    Notify(nullptr);
}

Dispatcher::Watch& Dispatcher::GetWatch(Connection* c) {
    Data::Map::iterator it = d_->map_.find(c);
    if (it == d_->map_.end())
        it = d_->map_.emplace(c, Watch()).first;
    return it->second;
}

void Dispatcher::DispatchOne(const std::chrono::milliseconds& timeout) {

    Connection* c = nullptr;
    if (!d_->notify_.pop_for(c, timeout)) {
        sLOG << "DispatchOne timeout";
        return;
    }

    if (c == nullptr) {
        sLOG << "DispatchOne interrupt";
        return;
    }

    sLOG << "DispatchOne run";

    std::unique_lock<std::mutex> d_lock(d_->mutex_);

    Data::Map::iterator it = d_->map_.find(c);
    if (it == d_->map_.end()) {
        sLOG << "DispatchOne expired connection?";
        return;
    }

    Watch& w = it->second;
    assert(w.active);

    std::unique_lock<std::mutex> c_lock(c->d_->mutex_);

    // check for readability
    if (w.read_cb.size() && c->d_->inbound_.size()) {

        while (c->d_->inbound_.size() && w.read_cb.size()) {
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
            c->d_->watcher_.erase(this);
            d_->map_.erase(it);
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
            c->d_->watcher_.erase(this);
            d_->map_.erase(it);
            return;
        }
    }
}

} // namespace mock
} // namespace net
} // namespace thrill

/******************************************************************************/
