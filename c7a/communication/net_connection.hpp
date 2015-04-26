/*******************************************************************************
 * c7a/communication/net_connection.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_COMMUNICATION_NET_CONNECTION_HEADER
#define C7A_COMMUNICATION_NET_CONNECTION_HEADER

#pragma once
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

#include <c7a/communication/socket.hpp>

namespace c7a {

#define NET_CLIENT_SUCCESS 0
#define NET_CLIENT_HEADER_RECEIVE_FAILED -1
#define NET_CLIENT_CONNECT_FAILED -2
#define NET_CLIENT_SOCKET_CREATION_FAILED -3
#define NET_CLIENT_NAME_RESOLVE_FAILED -4
#define NET_CLIENT_SEND_ERROR -5
#define NET_CLIENT_DATA_RECEIVE_FAILED -6

/*!
 * NetConnection is a message-based socket connection to another client (worker
 * or master). Messages are opaque byte strings with a length.
 */
class NetConnection : protected Socket
{
public:
    static const bool debug = true;

    static const bool SelfVerify = true;

    const int connectedWorker;

    NetConnection(int workerId)
        : connectedWorker(workerId)
    { }

    NetConnection(int existingSocket, int workerId)
        : Socket(existingSocket),
          connectedWorker(workerId)
    { }

    //! \name Send Functions
    //! \{

    //! Send a fixed-length type, possibly without length header.
    template <typename T>
    int Send(const T& value)
    {
        if (SelfVerify) {
            // for communication verification, send sizeof.
            size_t len = sizeof(value);
            if (send(&len, sizeof(len), MSG_MORE) != sizeof(len)) {
                return NET_CLIENT_SEND_ERROR;
            }
        }

        if (send(value, sizeof(value)) != (ssize_t)sizeof(value)) {
            return NET_CLIENT_SEND_ERROR;
        }

        return NET_CLIENT_SUCCESS;
    }

    //! Send a string buffer
    int SendString(const void* data, size_t len)
    {
        if (send(&len, sizeof(len), MSG_MORE) != sizeof(len)) {
            return NET_CLIENT_SEND_ERROR;
        }

        if (send(data, len) != (ssize_t)len) {
            return NET_CLIENT_SEND_ERROR;
        }

        return NET_CLIENT_SUCCESS;
    }

    //! Send a string message.
    int SendString(const std::string& message)
    {
        return SendString(message.data(), message.size());
    }

    //! \}

    //! \name Receive Functions
    //! \{

    //! Receive a fixed-length type, possibly without length header.
    template <typename T>
    int Receive(T& value)
    {
        if (SelfVerify) {
            // for communication verification, receive sizeof.
            size_t len = 0;
            if (recv(&len, sizeof(len)) != sizeof(len)) {
                return NET_CLIENT_HEADER_RECEIVE_FAILED;
            }

            // if this fails, then fixed-length type communication desynced.
            die_unequal(len, sizeof(value));
        }

        if (recv(&value, sizeof(value)) != (ssize_t)sizeof(value)) {
            return NET_CLIENT_SEND_ERROR;
        }

        return NET_CLIENT_SUCCESS;
    }

    //! Blocking receive string message from the connected socket.
    int ReceiveString(std::string* outdata)
    {
        size_t len = 0;

        if (recv(&len, sizeof(len)) != sizeof(len)) {
            return NET_CLIENT_HEADER_RECEIVE_FAILED;
        }

        if (len == 0)
            return NET_CLIENT_SUCCESS;

        outdata->resize(len);

        ssize_t ret = recv(const_cast<char*>(outdata->data()), len);

        if (ret != (ssize_t)len) {
            return NET_CLIENT_DATA_RECEIVE_FAILED;
        }

        return NET_CLIENT_SUCCESS;
    }

    //! \}

    int Connect(std::string address_, int port)
    {
        assert(fd_ == -1);

        fd_ = socket(AF_INET, SOCK_STREAM, 0);

        if (fd_ == -1) {
            return NET_CLIENT_SOCKET_CREATION_FAILED; //Socket creation failed.
        }

        if (inet_addr(address_.c_str()) == INADDR_NONE) {
            struct in_addr** addressList;
            struct hostent* resolved;

            if ((resolved = gethostbyname(address_.c_str())) == NULL) {
                return NET_CLIENT_NAME_RESOLVE_FAILED; //Host resolve failed.
            }

            addressList = (struct in_addr**)resolved->h_addr_list;
            serverAddress_.sin_addr = *addressList[0];
        }
        else {
            serverAddress_.sin_addr.s_addr = inet_addr(address_.c_str());
        }

        serverAddress_.sin_family = AF_INET;
        serverAddress_.sin_port = htons(port);

        if (connect(fd_, (struct sockaddr*)&serverAddress_, sizeof(serverAddress_)) < 0) {
            shutdown(fd_, SHUT_WR);
            fd_ = -1;
            return NET_CLIENT_CONNECT_FAILED;
        }

        return NET_CLIENT_SUCCESS;
    }

    void Close()
    {
        shutdown(fd_, 2); //2 == Stop reception and transmission
    }

private:
    std::string address_;
    struct sockaddr_in serverAddress_;

    friend class NetDispatcher;
};

} // namespace c7a

#endif // !C7A_COMMUNICATION_NET_CONNECTION_HEADER

/******************************************************************************/
