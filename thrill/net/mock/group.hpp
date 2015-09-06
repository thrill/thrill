/*******************************************************************************
 * thrill/net/mock/group.hpp
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

#include <thrill/common/delegate.hpp>
#include <thrill/net/dispatcher.hpp>

#include <condition_variable>
#include <deque>
#include <mutex>

namespace thrill {
namespace net {
namespace mock {

class Connection;

class Group : public net::GroupBase
{
    static const bool debug_data = true;

public:
    //! Mutex to lock access to message queues
    std::mutex mutex_;

    //! Condition variable to wake up threads waiting on messages.
    std::condition_variable cv_;

    //! type of message queue
    using DataQueue = std::deque<std::string>;

    //! inbound message queue from each of the network peers
    std::vector<DataQueue> inbound_;

    //! vector of peers for delivery of messages.
    std::vector<Group*> peers_;

    //! \name Synchronous Send and Receive Functions
    //! \{

    //! Send a buffer to peer tgt. Blocking, ... sort of.
    void Send(size_t tgt, std::string&& msg) {
        assert(tgt < peers_.size());

        sLOG1 << "Sending" << my_rank_ << "->" << tgt
              << "msg" << maybe_hexdump(msg.data(), msg.size());

        std::unique_lock<std::mutex> lock(peers_[tgt]->mutex_);
        peers_[tgt]->inbound_[my_rank_].emplace_back(std::move(msg));
        peers_[tgt]->cv_.notify_all();
    }

    //! Receive a buffer from peer src. Blocks until one is received!
    std::string Receive(size_t src) {
        assert(src < peers_.size());
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [=]() { return !inbound_[src].empty(); });
        std::string msg = std::move(inbound_[src].front());
        inbound_[src].pop_front();
        return msg;
    }

    //! \}

    Connection connection(size_t peer);

    size_t num_hosts() const final { return peers_.size(); }

    //! return hexdump or just <data> if not debugging
    static std::string maybe_hexdump(const void* data, size_t size) {
        if (debug_data)
            return common::hexdump(data, size);
        else
            return "<data>";
    }
};

class Connection final : public net::Connection
{
public:
    //! Reference to our group.
    Group& group_;

    //! Outgoing peer id of this Connection.
    size_t peer_;

    //! construct from mock::Group
    Connection(Group& group, size_t peer)
        : group_(group), peer_(peer) { }

    bool IsValid() const final { return true; }

    std::string ToString() const final {
        return "peer: " + std::to_string(peer_);
    }

    //! Send a string buffer
    ssize_t SyncSend(const void* data, size_t size, int /* flags */) final {
        group_.Send(peer_,
                    std::string(reinterpret_cast<const char*>(data), size));
        return size;
    }

    ssize_t SendOne(const void* data, size_t size) final {
        return SyncSend(data, size, 0);
    }

    //! Receive a buffer.
    ssize_t SyncRecv(void* out_data, size_t size) final {
        std::string msg = group_.Receive(peer_);
        die_unequal(msg.size(), size);
        char* out_cdata = reinterpret_cast<char*>(out_data);
        std::copy(msg.begin(), msg.end(), out_cdata);
        return size;
    }

    ssize_t RecvOne(void* out_data, size_t size) final {
        return SyncRecv(out_data, size);
    }
};

inline Connection Group::connection(size_t peer) {
    return Connection(*this, peer);
}

class Dispatcher : public net::Dispatcher
{
public:
    //! type for file descriptor readiness callbacks
    using Callback = common::delegate<bool()>;

    //! Register a buffered read callback and a default exception callback.
    void AddRead(net::Connection& c, const Callback& read_cb) final { }

    void AddWrite(net::Connection& c, const Callback& write_cb) final { }

    //! Run one iteration of dispatching select().
    void Dispatch(const std::chrono::milliseconds& timeout);
};

} // namespace mock
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MOCK_GROUP_HEADER

/******************************************************************************/
