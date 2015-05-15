/*******************************************************************************
 * c7a/net/net-connection.hpp
 *
 * Contains NetConnection a richer set of network point-to-point primitives.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_NET_CONNECTION_HEADER
#define C7A_NET_NET_CONNECTION_HEADER

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>

#include <c7a/net/lowlevel/socket.hpp>
#include <c7a/net/lowlevel/net_exception.hpp>

namespace c7a {
namespace net {

//! \addtogroup net Network Communication
//! \{

// Because Mac OSX does not know MSG_MORE.
#ifndef MSG_MORE
#define MSG_MORE 0
#endif

/*!
 * NetConnection is a rich point-to-point socket connection to another client
 * (worker, master, or whatever). Messages are fixed-length integral items or
 * opaque byte strings with a length.
 *
 * If any function fails to send or receive, then a NetException is thrown
 * instead of explicit error handling. If ever an error occurs, we probably have
 * to rebuild the whole network explicitly.
 */
class NetConnection : protected lowlevel::Socket
{
    static const bool debug = false;

    static const bool self_verify_ = true;

public:
    //! Construct NetConnection from a Socket
    explicit NetConnection(const Socket& s)
        : Socket(s)
    { } 

    explicit NetConnection()
        : Socket()
    { }


#if !C7A_NETCONNECTION_COPYABLE
    //! move-constructor
    NetConnection(NetConnection&& other) : Socket(other)
    { other.fd_ = -1; }

    //! move assignment-operator
    NetConnection& operator = (NetConnection&& other)
    {
        if (IsValid()) {
            sLOG1 << "Assignment-destruction of valid NetConnection" << this;
            Close();
        }
        Socket::operator = (other);
        other.fd_ = -1;
        return *this;
    }
#endif

    //! Check whether the contained file descriptor is valid.
    bool IsValid() const
    { return Socket::IsValid(); }

    //! Return the raw socket object for more low-level network programming.
    Socket & GetSocket()
    { return *this; }

    //! Return the raw socket object for more low-level network programming.
    const Socket & GetSocket() const
    { return *this; }

    //! Return the associated socket error
    int GetError() const
    { return Socket::GetError(); }

    //! Set socket to non-blocking
    int SetNonBlocking(bool non_blocking) const
    { return Socket::SetNonBlocking(non_blocking); }

    //! Return the socket peer address
    std::string GetPeerAddress() const
    { return Socket::GetPeerAddress().ToStringHostPort(); }

    bool operator == (const NetConnection& c) const
    { return GetSocket().fd() == c.GetSocket().fd(); }

    //! \name Send Functions
    //! \{

    //! Send a fixed-length type T (possibly without length header).
    template <typename T>
    void Send(const T& value)
    {
        static_assert(std::is_integral<T>::value,
                      "You only want to send integral types as raw values.");

        if (self_verify_) {
            // for communication verification, send sizeof.
            size_t len = sizeof(value);
            if (send(&len, sizeof(len), MSG_MORE) != sizeof(len))
                throw lowlevel::NetException("Error during Send", errno);
        }

        if (send(&value, sizeof(value)) != (ssize_t)sizeof(value))
            throw lowlevel::NetException("Error during Send", errno);
    }

    //! Send a string buffer
    void SendString(const void* data, size_t len)
    {
        if (send(&len, sizeof(len), MSG_MORE) != sizeof(len))
            throw lowlevel::NetException("Error during SendString", errno);

        if (send(data, len) != (ssize_t)len)
            throw lowlevel::NetException("Error during SendString", errno);
    }

    //! Send a string message.
    void SendString(const std::string& message)
    {
        SendString(message.data(), message.size());
    }

    //! \}

    //! \name Receive Functions
    //! \{

    //! Receive a fixed-length type, possibly without length header.
    template <typename T>
    void Receive(T* out_value)
    {
        static_assert(std::is_integral<T>::value,
                      "You only want to receive integral types as raw values.");

        if (self_verify_) {
            // for communication verification, receive sizeof.
            size_t len = 0;
            if (recv(&len, sizeof(len)) != sizeof(len))
                throw lowlevel::NetException("Error during Receive", errno);

            // if this fails, then fixed-length type communication desynced.
            die_unequal(len, sizeof(*out_value));
        }

        if (recv(out_value, sizeof(*out_value)) != (ssize_t)sizeof(*out_value))
            throw lowlevel::NetException("Error during Receive", errno);
    }

    //! Blocking receive string message from the connected socket.
    void ReceiveString(std::string* outdata)
    {
        size_t len = 0;

        if (recv(&len, sizeof(len)) != sizeof(len))
            throw lowlevel::NetException("Error during ReceiveString", errno);

        if (len == 0)
            return;

        outdata->resize(len);

        ssize_t ret = recv(const_cast<char*>(outdata->data()), len);

        if (ret != (ssize_t)len)
            throw lowlevel::NetException("Error during ReceiveString", errno);
    }

    //! \}

    //! Destruction of NetConnection should be explicitly done by a NetGroup or
    //! other network class.
    ~NetConnection()
    {
        if (IsValid()) {
            sLOG1 << "Destruction of valid NetConnection" << this;
            Close();
        }
    }

    //! Close this NetConnection
    void Close()
    {
        Socket::close();
    }

    //! make ostreamable
    friend std::ostream& operator << (std::ostream& os, const NetConnection& c)
    {
        os << "[NetConnection"
           << " fd=" << c.GetSocket().fd();

        if (c.IsValid())
            os << " peer=" << c.GetPeerAddress();

        return os << "]";
    }
};

// \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_NET_CONNECTION_HEADER

/******************************************************************************/
