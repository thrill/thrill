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
 * This file has no license. Only Chunk Norris can compile it.
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

//! \addtogroup net Network Communication
//! \{

enum ConnectionState : unsigned {
    Invalid, Connecting, TransportConnected, HelloReceived,
    HelloSent, WaitingForHello, Connected, Disconnected
};

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
    explicit Connection(const Socket& s)
        : socket_(s)
    { }

    //! Construct Connection from a Socket, with immediate
    //! initialization. (Currently used by tests).
    Connection(const Socket& s, size_t group_id, size_t peer_id)
        : socket_(s),
          group_id_(group_id), peer_id_(peer_id)
    { }

    //! move-constructor
    Connection(Connection&& other)
        : socket_(other.socket_),
          state_(other.state_),
          group_id_(other.group_id_),
          peer_id_(other.peer_id_) {
        other.socket_.Release();
        other.state_ = ConnectionState::Invalid;
    }

    //! move assignment-operator
    Connection& operator = (Connection&& other) {
        if (IsValid()) {
            sLOG1 << "Assignment-destruction of valid Connection" << this;
            Close();
        }
        socket_ = other.socket_;
        state_ = other.state_;
        group_id_ = other.group_id_;
        peer_id_ = other.peer_id_;

        other.socket_.Release();
        other.state_ = ConnectionState::Invalid;
        return *this;
    }

    /**
     * \brief Gets the state of this connection.
     */
    ConnectionState state() const {
        return state_;
    }

    /**
     * \brief Gets the id of the net group this connection is associated with.
     */
    size_t group_id() const {
        return group_id_;
    }

    /**
     * \brief Gets the id of the worker this connection is connected to.
     */
    size_t peer_id() const {
        return peer_id_;
    }

    // TODO(ej) Make setters internal/friend NetManager

    /**
     * \brief Sets the state of this connection.
     */
    void set_state(ConnectionState state) {
        this->state_ = state;
    }

    /**
     * \brief Sets the group id of this connection.
     */
    void set_group_id(size_t groupId) {
        this->group_id_ = groupId;
    }

    /**
     * \brief Sets the id of the worker this connection is connected to.
     */
    void set_peer_id(size_t peerId) {
        this->peer_id_ = peerId;
    }

    //! Check whether the contained file descriptor is valid.
    bool IsValid() const final
    { return socket_.IsValid(); }

    std::string ToString() const final { return std::to_string(GetSocket().fd()); }

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

    ssize_t SyncSend(const void* data, size_t size, int flags) final {
        return socket_.send(data, size, flags);
    }

    ssize_t SendOne(const void* data, size_t size) final {
        return socket_.send_one(data, size);
    }

    ssize_t SyncRecv(void* out_data, size_t size) final {
        return socket_.recv(out_data, size);
    }

    ssize_t RecvOne(void* out_data, size_t size) final {
        return socket_.recv_one(out_data, size);
    }

    //! Close this Connection
    void Close() {
        socket_.close();
    }

    //! make ostreamable
    friend std::ostream& operator << (std::ostream& os, const Connection& c) {
        os << "[tcp::Connection"
           << " fd=" << c.GetSocket().fd();

        if (c.IsValid())
            os << " peer=" << c.GetPeerAddress();

        return os << "]";
    }

protected:
    //! Underlying socket or connection handle.
    Socket socket_;

    //! The connection state of this connection in the Thrill network state
    //! machine.
    ConnectionState state_ = ConnectionState::Invalid;

    //! The id of the group this connection is associated with.
    size_t group_id_ = -1;

    //! The id of the worker this connection is connected to.
    size_t peer_id_ = -1;
};

// \}

} // namespace tcp
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_TCP_CONNECTION_HEADER

/******************************************************************************/
