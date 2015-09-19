/*******************************************************************************
 * thrill/net/tcp/connection.hpp
 *
 * Contains net::Connection, a richer set of network point-to-point primitives.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_TCP_CONNECTION_HEADER
#define THRILL_NET_TCP_CONNECTION_HEADER

#include <thrill/common/config.hpp>
#include <thrill/net/connection.hpp>
#include <thrill/net/tcp/socket.hpp>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>

namespace thrill {
namespace net {
namespace tcp {

//! \addtogroup net_tcp TCP Socket API
//! \{

enum ConnectionState : unsigned {
    Invalid, Connecting, TransportConnected, HelloReceived,
    HelloSent, WaitingForHello, Connected, Disconnected
};

// Because Mac OSX does not know MSG_MORE.
#ifndef MSG_MORE
#define MSG_MORE 0
#endif

/*!
 * Connection is a rich point-to-point socket connection to another client
 * (worker, master, or whatever). Messages are fixed-length integral items or
 * opaque byte strings with a length.
 *
 * If any function fails to send or receive, then a NetException is thrown
 * instead of explicit error handling. If ever an error occurs, we probably have
 * to rebuild the whole network explicitly.
 */
class Connection final : public net::Connection
{
    static const bool debug = false;

public:
    //! default construction, contains invalid socket
    Connection() = default;

    //! Construct Connection from a Socket
    explicit Connection(Socket&& s)
        : socket_(std::move(s))
    { }

    //! Construct Connection from a Socket, with immediate
    //! initialization. (Currently used by tests).
    Connection(Socket&& s, size_t group_id, size_t peer_id)
        : socket_(std::move(s)),
          group_id_(group_id), peer_id_(peer_id)
    { }

    //! move-constructor
    Connection(Connection&& other)
        : socket_(std::move(other.socket_)),
          state_(other.state_),
          group_id_(other.group_id_),
          peer_id_(other.peer_id_) {
        other.state_ = ConnectionState::Invalid;
    }

    //! move assignment-operator
    Connection& operator = (Connection&& other) {
        if (IsValid()) {
            sLOG1 << "Assignment-destruction of valid Connection" << this;
            Close();
        }
        socket_ = std::move(other.socket_);
        state_ = other.state_;
        group_id_ = other.group_id_;
        peer_id_ = other.peer_id_;

        other.state_ = ConnectionState::Invalid;
        return *this;
    }

    //! Gets the state of this connection.
    ConnectionState state() const
    { return state_; }

    //! Gets the id of the net group this connection is associated with.
    size_t group_id() const
    { return group_id_; }

    //! Gets the id of the worker this connection is connected to.
    size_t peer_id() const
    { return peer_id_; }

    //! Sets the state of this connection.
    void set_state(ConnectionState state)
    { state_ = state; }

    //! Sets the group id of this connection.
    void set_group_id(size_t groupId)
    { group_id_ = groupId; }

    //! Sets the id of the worker this connection is connected to.
    void set_peer_id(size_t peerId)
    { peer_id_ = peerId; }

    //! Check whether the contained file descriptor is valid.
    bool IsValid() const final
    { return socket_.IsValid(); }

    std::string ToString() const final
    { return std::to_string(GetSocket().fd()); }

    //! Return the raw socket object for more low-level network programming.
    Socket & GetSocket()
    { return socket_; }

    //! Return the raw socket object for more low-level network programming.
    const Socket & GetSocket() const
    { return socket_; }

    //! Return the associated socket error
    int GetError() const
    { return socket_.GetError(); }

    //! Set socket to non-blocking
    int SetNonBlocking(bool non_blocking) const
    { return socket_.SetNonBlocking(non_blocking); }

    //! Return the socket peer address
    std::string GetPeerAddress() const
    { return socket_.GetPeerAddress().ToStringHostPort(); }

    //! Checks wether two connections have the same underlying socket or not.
    bool operator == (const Connection& c) const
    { return GetSocket().fd() == c.GetSocket().fd(); }

    //! Destruction of Connection should be explicitly done by a NetGroup or
    //! other network class.
    ~Connection() {
        if (IsValid()) {
            Close();
        }
    }

    void SyncSend(const void* data, size_t size, Flags flags) final {
        int f = 0;
        if (flags & MsgMore) f |= MSG_MORE;
        if (socket_.send(data, size, f) != static_cast<ssize_t>(size))
            throw Exception("Error during SyncSend", errno);
    }

    ssize_t SendOne(const void* data, size_t size, Flags flags) final {
        int f = 0;
        if (flags & MsgMore) f |= MSG_MORE;
        return socket_.send_one(data, size, f);
    }

    void SyncRecv(void* out_data, size_t size) final {
        if (socket_.recv(out_data, size) != static_cast<ssize_t>(size))
            throw Exception("Error during SyncRecv", errno);
    }

    ssize_t RecvOne(void* out_data, size_t size) final {
        return socket_.recv_one(out_data, size);
    }

    //! Close this Connection
    void Close() {
        socket_.close();
    }

    //! make ostreamable
    std::ostream & output_ostream(std::ostream& os) const final {
        os << "[tcp::Connection"
           << " fd=" << GetSocket().fd();

        if (IsValid())
            os << " peer=" << GetPeerAddress();

        return os << "]";
    }

protected:
    //! Underlying socket or connection handle.
    Socket socket_;

    //! The connection state of this connection in the Thrill network state
    //! machine.
    ConnectionState state_ = ConnectionState::Invalid;

    //! The id of the group this connection is associated with.
    size_t group_id_ = size_t(-1);

    //! The id of the worker this connection is connected to.
    size_t peer_id_ = size_t(-1);
};

// \}

} // namespace tcp
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_TCP_CONNECTION_HEADER

/******************************************************************************/
