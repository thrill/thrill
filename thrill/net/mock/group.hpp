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

#include <condition_variable>
#include <deque>
#include <mutex>

namespace thrill {
namespace net {
namespace mock {

class Socket;

class Group
{
    static const bool debug_data = true;

public:
    //! our rank in the mock network
    size_t my_rank_;

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
        std::string msg = inbound_[src].front();
        inbound_[src].pop_front();
        return msg;
    }

    //! \}

    Socket connection(size_t peer);

    size_t my_host_rank() const { return my_rank_; }

    size_t num_hosts() const { return peers_.size(); }

    //! return hexdump or just <data> if not debugging
    static std::string maybe_hexdump(const void* data, size_t size) {
        if (debug_data)
            return common::hexdump(data, size);
        else
            return "<data>";
    }
};

class Socket
{
public:
    //! Reference to our group.
    Group& group_;

    //! Outgoing peer id of this Socket.
    size_t peer_;

    //! construct from mock::Group
    Socket(Group& group, size_t peer)
        : group_(group), peer_(peer) { }

    //! Send a string buffer
    void SendString(const void* data, size_t size) {
        group_.Send(
            peer_, std::string(reinterpret_cast<const char*>(data), size));
    }

    //! Send a serializable Item.
    template <typename T>
    void Send(const T& value) {
        static_assert(std::is_pod<T>::value,
                      "You only want to send POD types as raw values.");

        SendString(&value, sizeof(value));
    }

    //! Receive a fixed-length type, possibly without length header.
    template <typename T>
    void Receive(T* out_value) {
        static_assert(std::is_pod<T>::value,
                      "You only want to receive POD types as raw values.");

        std::string msg = group_.Receive(peer_);
        assert(msg.size() == sizeof(T));
        *out_value = *reinterpret_cast<const T*>(msg.data());
    }
};

inline Socket Group::connection(size_t peer) {
    return Socket(*this, peer);
}

} // namespace mock
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MOCK_GROUP_HEADER

/******************************************************************************/
