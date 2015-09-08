/*******************************************************************************
 * thrill/net/mock/group.hpp
 *
 * Implementation of a mock network which does no real communication. All
 * classes: Group, Connection, and Dispatcher are in this file since they are
 * tightly connected to provide thread-safety.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
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
    }

    bool IsValid() const final { return true; }

    std::string ToString() const final {
        return "peer: " + std::to_string(peer_);
    }

    void InboundMsg(net::Buffer&& msg);

    void SyncSend(const void* data, size_t size, int /* flags */ = 0) final;

    ssize_t SendOne(const void* data, size_t size) final {
        SyncSend(data, size);
        return size;
    }

    net::Buffer RecvNext() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [=]() { return !inbound_.empty(); });
        net::Buffer msg = std::move(inbound_.front());
        inbound_.pop_front();
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

protected:
    //! Reference to our group.
    Group* group_;

    //! Outgoing peer id of this Connection.
    size_t peer_;

    //! Mutex to lock access to inbound message queue
    std::mutex mutex_;

    //! Condition variable to wake up threads synchronously blocking on
    //! messages.
    std::condition_variable cv_;

    //! Array of watching dispatchers.
    std::set<Dispatcher*> watcher_;

    //! type of message queue
    using DataQueue = std::deque<net::Buffer>;

    //! inbound message queue the virtual network peer
    DataQueue inbound_;

    //! for access to watch lists and mutex.
    friend class Dispatcher;
};

/*!
 * Context for communication links to peers in a mock network.
 */
class Group final : public net::Group
{
    static const bool debug = false;
    static const bool debug_data = true;

public:
    /*!
     * Construct a mock network with num_hosts peers and deliver Group contexts
     * for each of them.
     */
    static std::vector<std::unique_ptr<Group> > ConstructLocalMesh(
        size_t num_hosts);

    //! Initialize a Group for the given size and rank
    Group(size_t my_rank, size_t group_size)
        : net::Group(my_rank) {
        peers_.resize(group_size);
        // create virtual connections
        conns_ = new Connection[group_size];
        for (size_t i = 0; i < group_size; ++i)
            conns_[i].Initialize(this, i);
    }

    ~Group() {
        delete[] conns_;
    }

    //! Send a buffer to peer tgt. Blocking, ... sort of.
    void Send(size_t tgt, net::Buffer&& msg) {
        assert(tgt < peers_.size());

        sLOG << "Sending" << my_rank_ << "->" << tgt
             << "msg" << maybe_hexdump(msg.data(), msg.size());

        peers_[tgt]->conns_[my_rank_].InboundMsg(std::move(msg));
    }

    net::Connection & connection(size_t peer) final {
        assert(peer < peers_.size());
        return conns_[peer];
    }

    size_t num_hosts() const final { return peers_.size(); }

    void Close() final { }

    //! return hexdump or just <data> if not debugging
    static std::string maybe_hexdump(const void* data, size_t size) {
        if (debug_data)
            return common::hexdump(data, size);
        else
            return "<data>";
    }

public:
    //! vector of peers for delivery of messages.
    std::vector<Group*> peers_;

    //! vector of virtual connection objects to remote peers
    Connection* conns_;
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
            // our virtual sockets are always writable: issue notification.
            Notify(&c);
        }
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

    void DispatchOne(const std::chrono::milliseconds& timeout) final {

        Connection* c;
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
        Watch& w = it->second;
        assert(w.active);

        std::unique_lock<std::mutex> c_lock(c->mutex_);

        // check for readability
        if (w.read_cb.size() && c->inbound_.size()) {

            while (c->inbound_.size() && w.read_cb.size()) {
                c_lock.unlock();
                d_lock.unlock();

                bool ret = w.read_cb.front()();

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

                bool ret = w.write_cb.front()();

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

protected:
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
