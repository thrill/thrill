/*******************************************************************************
 * /<file>
 *
 *
 ******************************************************************************/

#ifndef _NEW_HEADER
#define _NEW_HEADER

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
namespace communication {

#define MAX_BUF_SIZE 10000

#define NET_CLIENT_SUCCESS 0
#define NET_CLIENT_HEADER_RECEIVE_FAILED -1
#define NET_CLIENT_CONNECT_FAILED -2
#define NET_CLIENT_SOCKET_CREATION_FAILED -3
#define NET_CLIENT_NAME_RESOLVE_FAILED -4
#define NET_CLIENT_SEND_ERROR -5
#define NET_CLIENT_DATA_RECEIVE_FAILED -6

class NetConnection : public Socket {

public:
    static const bool debug = true;

    const int connectedWorker;

    NetConnection(int workerId)
        : connectedWorker(workerId)
    { }

    NetConnection(int existingSocket, int workerId)
        : Socket(existingSocket),
          connectedWorker(workerId)
    { }

    int Receive(void **buf, size_t *messageLen)
    {
        int res = receiveHeader(messageLen);
        if (res != NET_CLIENT_SUCCESS) { return res; }
        sLOG << "NetConnection::Receive() res1" << res
             << "messageLen" << *messageLen;

        if (*messageLen > 0) {
            res = receiveData(*messageLen, receiveBuffer_);
            if(res != NET_CLIENT_SUCCESS) { return res; }
        }

        *buf = receiveBuffer_;

        return NET_CLIENT_SUCCESS;
    }

    int Connect(std::string address_, int port) {
        assert(fd_ == -1);

        fd_ = socket(AF_INET, SOCK_STREAM, 0);

        if(fd_ == -1) {
            return NET_CLIENT_SOCKET_CREATION_FAILED; //Socket creation failed.
        }

        if(inet_addr(address_.c_str()) == INADDR_NONE) {
            struct in_addr **addressList;
            struct hostent *resolved;

            if((resolved = gethostbyname(address_.c_str())) == NULL) {
                return NET_CLIENT_NAME_RESOLVE_FAILED; //Host resolve failed.
            }

            addressList = (struct in_addr**)resolved->h_addr_list;
            serverAddress_.sin_addr = *addressList[0];

        } else {
            serverAddress_.sin_addr.s_addr = inet_addr(address_.c_str());
        }

        serverAddress_.sin_family = AF_INET;
        serverAddress_.sin_port = htons(port);

        if(connect(fd_, (struct sockaddr*)&serverAddress_, sizeof(serverAddress_)) < 0) {
            shutdown(fd_, SHUT_WR);
            fd_ = -1;
            return NET_CLIENT_CONNECT_FAILED;
        }

        return NET_CLIENT_SUCCESS;
    };

    int Send(void* data, size_t len)
    {
        if (send(&len, sizeof(len), MSG_MORE) != sizeof(len)) {
            return NET_CLIENT_SEND_ERROR;
        }

        if (send(data, len) != len) {
            return NET_CLIENT_SEND_ERROR;
        }

        return NET_CLIENT_SUCCESS;
    }

    void Close() {
        shutdown(fd_, 2); //2 == Stop reception and transmission
    }

private:
    std::string address_;
    struct sockaddr_in serverAddress_;
    char sendBuffer_[MAX_BUF_SIZE];
    char receiveBuffer_[MAX_BUF_SIZE];

    int receiveHeader(size_t *len)
    {
        if (Socket::recv((void*)len, sizeof(size_t)) != sizeof(size_t)) {
            return NET_CLIENT_HEADER_RECEIVE_FAILED;
        }
        return NET_CLIENT_SUCCESS;
    };

    friend class NetDispatcher;

    bool receiveData(size_t len, void* data) {
        size_t ret = Socket::recv(data, len);

        if(ret != len) {
            return NET_CLIENT_DATA_RECEIVE_FAILED;
        } else {
            return NET_CLIENT_SUCCESS;
        }
    };
};

}}

#endif // !_NEW_HEADER

/******************************************************************************/
