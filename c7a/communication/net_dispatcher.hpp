#pragma once
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string>
#include <cstring>
#include <vector>
#include <assert.h>
#include <thread>
#include <map>
#include "execution_endpoint.hpp"
#include "blocking_connection.hpp"
#include <sys/select.h>

namespace c7a {
namespace communication {
#define NET_SERVER_SUCCESS 0
#define NET_SERVER_INIT_FAILED -1
#define NET_SERVER_ACCEPT_FAILED -2
#define NET_SERVER_CLIENT_FAILED -3

struct VertexId; //Forward declaration

//TODO 4 Tobi: remove raw pointers
class NetDispatcher {
public:

    const ExecutionEndpoints endpoints;
    const unsigned int localId;
    const unsigned int masterId;

    NetDispatcher(unsigned int localId, ExecutionEndpoints endpoints)
        : endpoints(endpoints), localId(localId), masterId(0)
    {
        clients.resize(endpoints.size());
        serverSocket_ = -1;
    }

    int Initialize() {
        return InitializeClients();
    }

    int Send(unsigned int dest, void* data, size_t len) {
        return clients[dest]->Send(data, len);
    }

    int Receive(unsigned int src, void** data, size_t *len) {
        return clients[src]->Receive(data, len);
    }

    int ReceiveFromAny(unsigned int *src, void** data, size_t* len) {
        *src = select(0, &fd_set_, NULL, NULL, NULL);

        if (*src <= 0) {
            return NET_SERVER_CLIENT_FAILED;
        } else {
            return Receive(*src, data, len);
        }
    }

    void Close() {
        for(unsigned int i = 0; i < endpoints.size(); i++) {
            if(i != localId) {
                //remove file descriptor of client that disconnects
                FD_CLR(clients[i]->GetFileDescriptor(), &fd_set_);

                clients[i]->Close();
            }
        }
    }

private:
    int serverSocket_;
    struct sockaddr_in serverAddress_;
    fd_set fd_set_; //list of file descriptors of all clients

    std::vector<NetConnection*> clients;

    int InitializeClients() {
        struct sockaddr clientAddress;
        socklen_t clientAddressLen;

        serverSocket_ = socket(AF_INET, SOCK_STREAM, 0);

        assert(serverSocket_ >= 0);

        serverAddress_.sin_family = AF_INET;
        serverAddress_.sin_addr.s_addr = INADDR_ANY;
        serverAddress_.sin_port = htons(endpoints[localId].port);

        if(localId > 0) {
            if(bind(serverSocket_, (struct sockaddr *) &serverAddress_, sizeof(serverAddress_)) < 0) {
                return NET_SERVER_INIT_FAILED;
            }

            listen(serverSocket_, localId);

            //Accept connections of all hosts with lower ID.
            for(unsigned int i = 0; i < localId; i++) {
                int clientSocket = accept(serverSocket_, &clientAddress, &clientAddressLen);

                //add file descriptor to read set for poll
                FD_SET(clientSocket, &fd_set_);

                if(clientSocket <= 0) {
                    return NET_SERVER_ACCEPT_FAILED;
                }
                //Hosts will always connect in Order.
                clients[i] = new NetConnection(clientSocket, i);
            }
        }
        //shutdown(serverSocket_, SHUT_WR);

        //Connect to all hosts with larger ID (in order).
        for(unsigned int i = localId + 1; i < endpoints.size(); i++) {
            NetConnection* client = new NetConnection(i);
            //Wait until server opens.
            int ret = 0;
            do {
                ret = client->Connect(endpoints[i].host, endpoints[i].port);
                if(ret == NET_CLIENT_CONNECT_FAILED) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                else if(ret != NET_CLIENT_SUCCESS) {
                    return NET_SERVER_CLIENT_FAILED;
                }
            } while(ret == NET_CLIENT_CONNECT_FAILED);
            clients[i] = client;
        }


        clients[localId] = NULL;

        //We finished connecting. :)
        return NET_SERVER_SUCCESS;

    };

};
}}
