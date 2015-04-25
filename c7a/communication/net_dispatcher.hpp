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

    NetDispatcher(unsigned int localId, ExecutionEndpoints endpoints)
        : endpoints(endpoints), localId(localId)
    {
        clients.resize(endpoints.size());
        serverSocket_ = -1;
    }

    int Initialize() {
        return InitializeClients();
    }

    int Send(int dest, void* data, size_t len) {
        return clients[dest]->Send(data, len);
    }

    int Receive(int src, void** data, size_t *len) {
        return clients[src]->Receive(data, len);
    }

    int ReceiveFromAny(void** data, int* len) {
        FD_ZERO(&m_readset);
        FD_ZERO(&m_writeset);
        FD_ZERO(&m_exceptset);

        for(auto& client : clients)
        {
            FD_SET(clients, &m_readset);
        }

        struct timeval timeout;

        timeout.tv_usec = (msec % 1000) * 1000;
        timeout.tv_sec = msec / 1000;

#ifdef __linux__
        // linux supports reading eslaped time from timeout

        int r = ::select(maxfd, &m_readset, &m_writeset, &m_exceptset, &timeout);

        m_elapsed = msec - (timeout.tv_sec * 1000 + timeout.tv_usec / 1000);

#else
        // else we have to do the gettimeofday calls ourselves

        struct timeval selstart, selfinish;

        gettimeofday(&selstart, NULL);
        int r = ::select(maxfd, &m_readset, &m_writeset, &m_exceptset, &timeout);
        gettimeofday(&selfinish, NULL);

        m_elapsed = (selfinish.tv_sec - selstart.tv_sec) * 1000;
        m_elapsed += (selfinish.tv_usec - selstart.tv_usec) / 1000;

        // int selsays = msec - (timeout.tv_sec * 1000 + timeout.tv_usec / 1000);

        Socket::trace("Spent %d msec in select. select says %d\n", m_elapsed, selsays);

#endif

        if (r < 0) {
        Socket::trace_error("Select() failed: %s\n", strerror(errno));
        }
        else if (r == 0) {
        return NULL;
        }
        else {
        for(socketlist_type::iterator s = m_socketlist.begin(); s != m_socketlist.end(); ++s)
        {
            if (readable  && FD_ISSET((*s)->get_fd(), &m_readset)) return *s;
            if (writeable && FD_ISSET((*s)->get_fd(), &m_writeset)) return *s;

            if (FD_ISSET((*s)->get_fd(), &m_exceptset)) return *s;
        }
        }
    }

    void Close() {
        for(unsigned int i = 0; i < endpoints.size(); i++) {
            if(i != localId_) {
                //remove file descriptor of client that disconnects
                FD_CLR(clients[i]->GetFiledescriptor(), &fd_set_)

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
