/*******************************************************************************
 * c7a/net/net-connection.hpp
 *
 * Contains NetConnection a richer set of network point-to-point primitives.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#ifndef C7A_NET_NET_CONNECTION_HEADER
#define C7A_NET_NET_CONNECTION_HEADER

#include <c7a/net/socket.hpp>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>

#include <c7a/net/socket.hpp>
#include <c7a/net/net-exception.hpp>
#include <string>
#include <thread>

namespace c7a {

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
class NetConnection : protected Socket
{
    static const bool debug = true;

    static const bool self_verify_ = true;

public:
    //! Construct NetConnection from a Socket
    explicit NetConnection(const Socket& s = Socket())
        : Socket(s)
    { }

    //! Return the associated file descriptor
    int GetFileDescriptor() const
    { return Socket::GetFileDescriptor(); }

    //! Return the raw socket object for more low-level network programming.
    Socket & GetSocket()
    { return *this; }

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
                throw NetException("Error during Send", errno);
        }

        if (send(&value, sizeof(value)) != (ssize_t)sizeof(value))
            throw NetException("Error during Send", errno);
    }

    //! Send a string buffer
    void SendString(const void* data, size_t len)
    {
        if (send(&len, sizeof(len), MSG_MORE) != sizeof(len))
            throw NetException("Error during SendString", errno);

        if (send(data, len) != (ssize_t)len)
            throw NetException("Error during SendString", errno);
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
                throw NetException("Error during Receive", errno);

            // if this fails, then fixed-length type communication desynced.
            die_unequal(len, sizeof(*out_value));
        }

        if (recv(out_value, sizeof(*out_value)) != (ssize_t)sizeof(*out_value))
            throw NetException("Error during Receive", errno);
    }

    //! Blocking receive string message from the connected socket.
    void ReceiveString(std::string* outdata)
    {
        size_t len = 0;

        if (recv(&len, sizeof(len)) != sizeof(len))
            throw NetException("Error during ReceiveString", errno);

        if (len == 0)
            return;

        outdata->resize(len);

        ssize_t ret = recv(const_cast<char*>(outdata->data()), len);

        if (ret != (ssize_t)len)
            throw NetException("Error during ReceiveString", errno);
    }

    //! \}

    //! Close this NetConnection
    void Close()
    {
        shutdown();
    }
};

// \}

} // namespace c7a

#endif // !C7A_NET_NET_CONNECTION_HEADER

/******************************************************************************/
