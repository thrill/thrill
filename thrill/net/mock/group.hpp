/*******************************************************************************
 * thrill/net/mock/group.hpp
 *
 * Implementation of a mock network which does no real communication. All
 * classes: Group, Connection, and Dispatcher are in this file since they are
 * tightly interdependent to provide thread-safety.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_MOCK_GROUP_HEADER
#define THRILL_NET_MOCK_GROUP_HEADER

#include <thrill/common/concurrent_bounded_queue.hpp>
#include <thrill/common/delegate.hpp>
#include <thrill/net/dispatcher.hpp>
#include <thrill/net/group.hpp>

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

//! \addtogroup net_mock Mock Network API
//! \ingroup net
//! \{

class Group;
class Dispatcher;

/*!
 * A virtual connection through the mock network: each Group has p Connections
 * to its peers. The Connection hands over packets to the peers via SyncSend(),
 * and receives them as InboundMsg().
 */
class Connection final : public net::Connection
{
public:
    //! construct from mock::Group
    void Initialize(Group* group, size_t peer) {
        group_ = group;
        peer_ = peer;
        is_loopback_ = true;
    }

    //! Method which is called by other peers to enqueue a message.
    void InboundMsg(net::Buffer&& msg);

    //! \name Base Status Functions
    //! \{

    bool IsValid() const final { return true; }

    std::string ToString() const final {
        return "peer: " + std::to_string(peer_);
    }

    std::ostream & OutputOstream(std::ostream& os) const final {
        return os << "[mock::Connection"
                  << " group=" << group_
                  << " peer=" << peer_
                  << "]";
    }

    //! \}

    //! \name Send Functions
    //! \{

    void SyncSend(
        const void* data, size_t size, Flags /* flags */ = NoFlags) final;

    ssize_t SendOne(
        const void* data, size_t size, Flags flags = NoFlags) final {
        SyncSend(data, size, flags);
        return size;
    }

    //! \}

    //! \name Receive Functions
    //! \{

    //! some-what internal function to extract the next packet from the queue.
    net::Buffer RecvNext() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [=]() { return !inbound_.empty(); });
        net::Buffer msg = std::move(inbound_.front());
        inbound_.pop_front();

        // set errno : success (other syscalls may have failed)
        errno = 0;

        return msg;
    }

    void SyncRecv(void* out_data, size_t size) final {
        net::Buffer msg = RecvNext();
        die_unequal(msg.size(), size);
        char* out_cdata = reinterpret_cast<char*>(out_data);
        std::copy(msg.begin(), msg.end(), out_cdata);
    }

    ssize_t RecvOne(void* out_data, size_t size) final {
        SyncRecv(out_data, size);
        return size;
    }

    //! \}

private:
    //! Reference to our group.
    Group* group_;

    //! Outgoing peer id of this Connection.
    size_t peer_;

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

    //! for access to watch lists and mutex.
    friend class Dispatcher;
};

/*!
 * The central object of a mock network: the Group containing links to other
 * mock Group forming the network. Note that there is no central object
 * containing all Groups.
 */
class Group final : public net::Group
{
    static const bool debug = false;
    static const bool debug_data = true;

public:
    //! \name Base Functions
    //! \{

    //! Initialize a Group for the given size and rank
    Group(size_t my_rank, size_t group_size)
        : net::Group(my_rank) {
        peers_.resize(group_size);
        // create virtual connections, due to complications with non-movable
        // mutexes, use a plain new.
        conns_ = new Connection[group_size];
        for (size_t i = 0; i < group_size; ++i)
            conns_[i].Initialize(this, i);
    }

    ~Group() {
        delete[] conns_;
    }

    size_t num_hosts() const final { return peers_.size(); }

    net::Connection & connection(size_t peer) final {
        assert(peer < peers_.size());
        return conns_[peer];
    }

    void Close() final { }

    mem::mm_unique_ptr<net::Dispatcher> ConstructDispatcher(
        mem::Manager& mem_manager) const final;

    //! \}

    /*!
     * Construct a mock network with num_hosts peers and deliver Group contexts
     * for each of them.
     */
    static std::vector<std::unique_ptr<Group> > ConstructLoopbackMesh(
        size_t num_hosts);

    //! return hexdump or just [data] if not debugging
    static std::string MaybeHexdump(const void* data, size_t size) {
        if (debug_data)
            return common::Hexdump(data, size);
        else
            return "[data]";
    }

private:
    //! vector of peers for delivery of messages.
    std::vector<Group*> peers_;

    //! vector of virtual connection objects to remote peers
    Connection* conns_;

    //! Send a buffer to peer tgt. Blocking, ... sort of.
    void Send(size_t tgt, net::Buffer&& msg) {
        assert(tgt < peers_.size());

        if (debug) {
            sLOG << "Sending" << my_rank_ << "->" << tgt
                 << "msg" << MaybeHexdump(msg.data(), msg.size());
        }

        peers_[tgt]->conns_[my_rank_].InboundMsg(std::move(msg));
    }

    //! for access to Send()
    friend class Connection;
};

/*!
 * A virtual Dispatcher which waits for messages to arrive in the mock
 * network. It is implemented as a ConcurrentBoundedQueue, into which
 * Connections lay notification events.
 */
class Dispatcher final : public net::Dispatcher
{
    static const bool debug = false;

public:
    //! type for file descriptor readiness callbacks
    using Callback = AsyncCallback;

    explicit Dispatcher(mem::Manager& mem_manager)
        : net::Dispatcher(mem_manager)
    { }

    //! Register a buffered read callback and a default exception callback.
    void AddRead(net::Connection& _c, const Callback& read_cb) final {
        assert(dynamic_cast<Connection*>(&_c));
        Connection& c = static_cast<Connection&>(_c);

        std::unique_lock<std::mutex> d_lock(mutex_);
        Watch& w = GetWatch(&c);
        w.read_cb.emplace_back(read_cb);
        if (!w.active) {
            std::unique_lock<std::mutex> c_lock(c.mutex_);
            c.watcher_.insert(this);
            w.active = true;
            // if already have a packet, issue notification.
            if (c.inbound_.size())
                Notify(&c);
        }
    }

    void AddWrite(net::Connection& _c, const Callback& write_cb) final {
        assert(dynamic_cast<Connection*>(&_c));
        Connection& c = static_cast<Connection&>(_c);

        std::unique_lock<std::mutex> d_lock(mutex_);
        Watch& w = GetWatch(&c);
        w.write_cb.emplace_back(write_cb);
        if (!w.active) {
            std::unique_lock<std::mutex> c_lock(c.mutex_);
            c.watcher_.insert(this);
            w.active = true;
        }
        // our virtual sockets are always writable: issue notification.
        Notify(&c);
    }

    void Cancel(net::Connection&) final {
        abort();
    }

    void Notify(Connection* c) {
        notify_.emplace(c);
    }

    void Interrupt() final {
        Notify(nullptr);
    }

    void DispatchOne(const std::chrono::milliseconds& timeout) final;

private:
    //! Mutex to lock access to watch lists
    std::mutex mutex_;

    //! Notification queue for Dispatch
    common::OurConcurrentBoundedQueue<Connection*> notify_;

    //! callback vectors per watched connection
    struct Watch
    {
        //! boolean check whether Watch is registered at Connection
        bool                    active = false;
        //! queue of callbacks for fd.
        mem::mm_deque<Callback> read_cb, write_cb;
        //! only one exception callback for the fd.
        Callback                except_cb;

        explicit Watch(mem::Manager& mem_manager)
            : read_cb(mem::Allocator<Callback>(mem_manager)),
              write_cb(mem::Allocator<Callback>(mem_manager)) { }
    };

    using Map = std::map<Connection*, Watch>;

    Map map_;

    Watch & GetWatch(Connection* c) {
        Map::iterator it = map_.find(c);
        if (it == map_.end())
            it = map_.emplace(c, Watch(mem_manager_)).first;
        return it->second;
    }
};

//! \}

} // namespace mock
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MOCK_GROUP_HEADER

/******************************************************************************/
