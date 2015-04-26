/*******************************************************************************
 * c7a/communication/net-connection.hpp
 *
 * Contains NetConnection a richer set of network point-to-point primitives.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_COMMUNICATION_NET_CONNECTION_HEADER
#define C7A_COMMUNICATION_NET_CONNECTION_HEADER

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <string>
#include <cstring>
#include <thread>
#include <assert.h>
#include <cstdio>
#include <cerrno>
#include <stdexcept>

#include <c7a/net/socket.hpp>

namespace c7a {

class NetException : public std::runtime_error
{
public:
    NetException(const std::string& what)
        : std::runtime_error(what)
    { }

    NetException(const std::string& what, int _errno)
        : std::runtime_error(what + ": " + strerror(_errno))
    { }
};

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
public:
    static const bool debug = true;

    static const bool self_verify_ = true;

    //! Construct NetConnection from a Socket
    NetConnection(const Socket& s = Socket())
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
        if (self_verify_) {
            // for communication verification, send sizeof.
            size_t len = sizeof(value);
            if (send(&len, sizeof(len), MSG_MORE) != sizeof(len))
                throw NetException("Error during Send", errno);
        }

        if (send(value, sizeof(value)) != (ssize_t)sizeof(value))
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
    void Receive(T& value)
    {
        if (self_verify_) {
            // for communication verification, receive sizeof.
            size_t len = 0;
            if (recv(&len, sizeof(len)) != sizeof(len))
                throw NetException("Error during Receive", errno);

            // if this fails, then fixed-length type communication desynced.
            die_unequal(len, sizeof(value));
        }

        if (recv(&value, sizeof(value)) != (ssize_t)sizeof(value))
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

    void Connect(const std::string& address)
    {
        assert(fd_ == -1);

        SocketAddress sa(address);

        // if (inet_addr(address_.c_str()) == INADDR_NONE) {
        //     struct in_addr** addressList;
        //     struct hostent* resolved;

        //     if ((resolved = gethostbyname(address_.c_str())) == NULL) {
        //         return NET_CLIENT_NAME_RESOLVE_FAILED; //Host resolve failed.
        //     }

        //     addressList = (struct in_addr**)resolved->h_addr_list;
        //     serverAddress_.sin_addr = *addressList[0];
        // }
        // else {
        //     serverAddress_.sin_addr.s_addr = inet_addr(address_.c_str());
        // }

        // serverAddress_.sin_family = AF_INET;
        // serverAddress_.sin_port = htons(port);

        if (!connect(sa)) {
            shutdown();
            fd_ = -1;
            throw NetException("Error during Connect(" + address + ")", errno);
        }
    }

    //! Close this NetConnection
    void Close()
    {
        shutdown();
    }
};

} // namespace c7a

#endif // !C7A_COMMUNICATION_NET_CONNECTION_HEADER

/******************************************************************************/
