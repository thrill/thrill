/*******************************************************************************
 * c7a/communication/net_dispatcher.hpp
 *
 *
 ******************************************************************************/

#ifndef C7A_COMMUNICATION_NET_DISPATCHER_HEADER
#define C7A_COMMUNICATION_NET_DISPATCHER_HEADER

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string>
#include <cstring>
#include <vector>
#include <assert.h>
#include <thread>
#include <map>
#include <c7a/communication/execution_endpoint.hpp>
#include <c7a/communication/net_connection.hpp>
#include <sys/select.h>

namespace c7a {

#define NET_SERVER_SUCCESS 0
#define NET_SERVER_INIT_FAILED -1
#define NET_SERVER_ACCEPT_FAILED -2
#define NET_SERVER_CLIENT_FAILED -3

struct VertexId; //Forward declaration

//TODO 4 Tobi: remove raw pointers
/*!
 * Collection of endpoints to workers and the master.
 */
class NetDispatcher
{
public:
    static const bool debug = true;

    const ExecutionEndpoints endpoints;
    const unsigned int localId;
    const unsigned int masterId;

    NetDispatcher(unsigned int localId, const ExecutionEndpoints& endpoints)
        : endpoints(endpoints), localId(localId), masterId(0)
    {
        clients.resize(endpoints.size());
        serverSocket_ = -1;
    }

    int Initialize()
    {
        return InitializeClients();
    }

    /*!
     * Sends a message (data,len) to worker dest, returns status code.
     */
    int Send(unsigned int dest, const void* data, size_t len)
    {
        LOG << "NetDispatcher::Send"
            << " " << localId << " -> " << dest
            << " data=" << hexdump(data, len)
            << " len=" << len;

        return clients[dest]->SendString(data, len);
    }

    /*!
     * Sends a message to worker dest, returns status code.
     */
    int Send(unsigned int dest, const std::string& message)
    {
        return Send(dest, message.data(), message.size());
    }

    /*!
     * Receives a message into (data,len) from worker src, returns status code.
     */
    int Receive(unsigned int src, std::string* outdata)
    {
        LOG << "NetDispatcher::Receive src=" << src;

        int r = clients[src]->ReceiveString(outdata);

        LOG << "done NetDispatcher::Receive"
            << " " << src << " -> " << localId
            << " data=" << hexdump(*outdata);

        return r;
    }

    /*!
     * Receives a message from any worker into (data,len), puts worker id in
     * *src, and returns status code.
     */
    int ReceiveFromAny(unsigned int* src, std::string* outdata)
    {
        fd_set fd_set;
        int max_fd = 0;

        FD_ZERO(&fd_set);

        // add file descriptor to read set for poll TODO(ts): make this faster
        // (somewhen)

        sLOG << "--- NetDispatcher::ReceiveFromAny() - select():";

        for (size_t i = 0; i != endpoints.size(); ++i)
        {
            // skip our own client connection
            if (i == localId) continue;

            int fd = clients[i]->GetFileDescriptor();
            FD_SET(fd, &fd_set);
            max_fd = std::max(max_fd, fd);
            sLOG << "select from fd=" << fd;
        }

        int retval = select(max_fd + 1, &fd_set, NULL, NULL, NULL);

        if (retval < 0) {
            perror("select()");
            abort();
            return NET_SERVER_CLIENT_FAILED;
        }
        else if (retval == 0) {
            perror("select() TIMEOUT");
            abort();
            return NET_SERVER_CLIENT_FAILED;
        }

        for (size_t i = 0; i < endpoints.size(); i++)
        {
            // skip our own client connection
            if (i == localId) continue;

            int fd = clients[i]->GetFileDescriptor();

            if (FD_ISSET(fd, &fd_set))
            {
                sLOG << "select() readable fd" << fd;

                *src = i;
                return Receive(i, outdata);
            }
        }

        sLOG << "Select() returned but no fd was readable.";

        return NET_SERVER_SUCCESS;
    }

    void Close()
    {
        for (unsigned int i = 0; i < endpoints.size(); i++) {
            if (i != localId) {
                clients[i]->Close();
            }
        }
    }

private:
    int serverSocket_;
    struct sockaddr_in serverAddress_;

    std::vector<NetConnection*> clients;

    int InitializeClients()
    {
        struct sockaddr clientAddress;
        socklen_t clientAddressLen;

        serverSocket_ = socket(AF_INET, SOCK_STREAM, 0);

        assert(serverSocket_ >= 0);

        serverAddress_.sin_family = AF_INET;
        serverAddress_.sin_addr.s_addr = INADDR_ANY;
        serverAddress_.sin_port = htons(endpoints[localId].port);

        int sockoptflag = 1;

        if (setsockopt(serverSocket_, SOL_SOCKET, SO_REUSEPORT,
                       &sockoptflag, sizeof(sockoptflag)) != 0)
        {
            perror("Cannot set SO_REUSEPORT on socket fd");
            return NET_SERVER_INIT_FAILED;
        }

        if (localId > 0) {
            if (bind(serverSocket_,
                     (struct sockaddr*)&serverAddress_,
                     sizeof(serverAddress_)) < 0)
            {
                return NET_SERVER_INIT_FAILED;
            }

            listen(serverSocket_, localId);

            //Accept connections of all hosts with lower ID.
            for (unsigned int i = 0; i < localId; i++) {
                int fd = accept(serverSocket_, &clientAddress, &clientAddressLen);

                if (fd <= 0) {
                    return NET_SERVER_ACCEPT_FAILED;
                }
                //Hosts will always connect in Order.
                clients[i] = new NetConnection(fd, i);
            }
        }
        //shutdown(serverSocket_, SHUT_WR);

        //Connect to all hosts with larger ID (in order).
        for (unsigned int i = localId + 1; i < endpoints.size(); i++) {
            NetConnection* client = new NetConnection(i);
            //Wait until server opens.
            int ret = 0;
            do {
                ret = client->Connect(endpoints[i].host, endpoints[i].port);
                if (ret == NET_CLIENT_CONNECT_FAILED) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                else if (ret != NET_CLIENT_SUCCESS) {
                    return NET_SERVER_CLIENT_FAILED;
                }
            } while (ret == NET_CLIENT_CONNECT_FAILED);
            clients[i] = client;
        }

        clients[localId] = NULL;

        //We finished connecting. :)
        return NET_SERVER_SUCCESS;
    }
};

} // namespace c7a

#endif // !C7A_COMMUNICATION_NET_DISPATCHER_HEADER

/******************************************************************************/
