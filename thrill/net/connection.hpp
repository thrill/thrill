/*******************************************************************************
 * thrill/net/connection.hpp
 *
 * Contains net::Connection, a richer set of network point-to-point primitives.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_CONNECTION_HEADER
#define C7A_NET_CONNECTION_HEADER

#include <thrill/common/config.hpp>
#include <thrill/net/buffer.hpp>
#include <thrill/net/exception.hpp>
#include <thrill/net/lowlevel/socket.hpp>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>

namespace c7a {
namespace net {

//! \addtogroup net Network Communication
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
class Connection
{
    static const bool debug = false;

    static const bool self_verify_ = common::g_self_verify;

    using Socket = lowlevel::Socket;

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
    bool IsValid() const
    { return socket_.IsValid(); }

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

    //! \name Send Functions
    //! \{

    //! Send a fixed-length type T (possibly without length header).
    template <typename T>
    void Send(const T& value) {
        static_assert(std::is_pod<T>::value,
                      "You only want to send POD types as raw values.");

        if (self_verify_) {
            // for communication verification, send sizeof.
            size_t len = sizeof(value);
            if (socket_.send(&len, sizeof(len), MSG_MORE) != sizeof(len))
                throw Exception("Error during Send", errno);
        }

        if (socket_.send(&value, sizeof(value))
            != static_cast<ssize_t>(sizeof(value)))
            throw Exception("Error during Send", errno);
    }

    //! Send a string buffer
    void SendString(const void* data, size_t len) {
        if (socket_.send(&len, sizeof(len), MSG_MORE) != sizeof(len))
            throw Exception("Error during SendString", errno);

        if (socket_.send(data, len) != static_cast<ssize_t>(len))
            throw Exception("Error during SendString", errno);
    }

    //! Send a string message.
    void SendString(const std::string& message) {
        SendString(message.data(), message.size());
    }

    //! \}

    //! \name Receive Functions
    //! \{

    //! Receive a fixed-length type, possibly without length header.
    template <typename T>
    void Receive(T* out_value) {
        static_assert(std::is_pod<T>::value,
                      "You only want to receive POD types as raw values.");

        if (self_verify_) {
            // for communication verification, receive sizeof.
            size_t len = 0;
            if (socket_.recv(&len, sizeof(len)) != sizeof(len))
                throw Exception("Error during Receive", errno);

            // if this fails, then fixed-length type communication desynced.
            die_unequal(len, sizeof(*out_value));
        }

        if (socket_.recv(out_value, sizeof(*out_value))
            != static_cast<ssize_t>(sizeof(*out_value)))
            throw Exception("Error during Receive", errno);
    }

    //! Blocking receive string message from the connected socket.
    void ReceiveString(std::string* outdata) {
        size_t len = 0;

        if (socket_.recv(&len, sizeof(len)) != sizeof(len))
            throw Exception("Error during ReceiveString", errno);

        if (len == 0)
            return;

        outdata->resize(len);

        ssize_t ret = socket_.recv(const_cast<char*>(outdata->data()), len);

        if (ret != static_cast<ssize_t>(len))
            throw Exception("Error during ReceiveString", errno);
    }

    //! \}

    //! Destruction of Connection should be explicitly done by a NetGroup or
    //! other network class.
    ~Connection() {
        if (IsValid()) {
            Close();
        }
    }

    //! Close this Connection
    void Close() {
        socket_.close();
    }

    //! make ostreamable
    friend std::ostream& operator << (std::ostream& os, const Connection& c) {
        os << "[Connection"
           << " fd=" << c.GetSocket().fd();

        if (c.IsValid())
            os << " peer=" << c.GetPeerAddress();

        return os << "]";
    }

protected:
    //! Underlying socket.
    Socket socket_;

    //! The connection state of this connection in the c7a network state
    //! machine.
    ConnectionState state_ = ConnectionState::Invalid;

    //! The id of the group this connection is associated with.
    size_t group_id_ = -1;

    //! The id of the worker this connection is connected to.
    size_t peer_id_ = -1;
};

// \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_CONNECTION_HEADER

/******************************************************************************/
