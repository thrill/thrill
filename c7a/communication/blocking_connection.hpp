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

class NetConnection {


public:

    const int connectedWorker;

    NetConnection(int workerId): connectedWorker(workerId) {
        sock_ = -1;
    }

    NetConnection(int existingSocket, int workerId)
        : connectedWorker(workerId)
          , sock_(existingSocket) {

    }

    int Receive(void **buf, size_t *messageLen) {
        int res = receiveHeader
            (messageLen);
        if(res != NET_CLIENT_SUCCESS) { return res; }

        if(messageLen > 0) {
            res = receiveData(*messageLen, receiveBuffer_);
            if(res != NET_CLIENT_SUCCESS) { return res; }
        }

        *buf = receiveBuffer_;

        return NET_CLIENT_SUCCESS;
    }

    int Connect(std::string address_, int port) {
        assert(sock_ == -1);

        sock_ = socket(AF_INET, SOCK_STREAM, 0);

        if(sock_ == -1) {
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

        if(connect(sock_, (struct sockaddr*)&serverAddress_, sizeof(serverAddress_)) < 0) {
            shutdown(sock_, SHUT_WR);
            sock_ = -1;
            return NET_CLIENT_CONNECT_FAILED;
        }

        return NET_CLIENT_SUCCESS;
    };

    int Send(void* data, size_t len) {
        //TODO make this zero copy
        memcpy(sendBuffer_, &len, sizeof(size_t));
        memcpy(sendBuffer_ + sizeof(int), data, len);
        if(send(sock_, sendBuffer_, len + sizeof(int), 0) < 0) {
            return NET_CLIENT_SEND_ERROR;
        }
        return NET_CLIENT_SUCCESS;
    }

    void Close() {
        shutdown(sock_, 2); //2 == Stop reception and transmission
    }

private:
    int sock_;
    std::string address_;
    struct sockaddr_in serverAddress_;
    char sendBuffer_[MAX_BUF_SIZE];
    char receiveBuffer_[MAX_BUF_SIZE];

    int receiveHeader(size_t *len) {

        if(recv(sock_, (void*)len, sizeof(size_t), 0) != sizeof(size_t)) {
            return NET_CLIENT_HEADER_RECEIVE_FAILED;
        }
        return NET_CLIENT_SUCCESS;
    };

    friend class NetDispatcher;
    int GetFileDescriptor() {
        return sock_;
    }

    bool receiveData(size_t len, void* data) {
        size_t ret = recv(sock_, data, len, 0);

        if(ret != len) {
            return NET_CLIENT_DATA_RECEIVE_FAILED;
        } else {
            return NET_CLIENT_SUCCESS;
        }
    };
};

}}
