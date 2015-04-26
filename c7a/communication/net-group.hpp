/*******************************************************************************
 * c7a/communication/net-group.hpp
 *
 * NetGroup is a collection of NetConnections providing simple MPI-like
 * collectives and point-to-point communication.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_COMMUNICATION_NET_GROUP_HEADER
#define C7A_COMMUNICATION_NET_GROUP_HEADER

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
#include <c7a/communication/net-connection.hpp>
#include <sys/select.h>

namespace c7a {

typedef unsigned int ClientId;

/*!
 * Collection of NetConnections to workers, allow point-to-point client
 * communication and simple collectives like MPI.
 */
class NetGroup
{
public:
    //! Construct a mock NetGroup using a complete graph of local stream sockets
    //! for testing, and starts a thread for each client, which gets passed the
    //! NetGroup object. \internal
    static void ExecuteLocalMock(
        size_t num_clients, const std::function<void(NetGroup*)>& thread_function);

    //! non-copyable: delete copy-constructor
    NetGroup(const NetGroup&) = delete;
    //! non-copyable: delete assignment operator
    NetGroup& operator = (const NetGroup&) = delete;

    static const bool debug = true;

    const ExecutionEndpoints endpoints;

    NetGroup(unsigned int my_id, const ExecutionEndpoints& endpoints)
        : endpoints(endpoints),
          my_rank_(my_id),
          connections_(endpoints.size())
    { }

    //! Return NetConnection to client id.
    NetConnection & GetConnection(ClientId id)
    {
        if (id >= connections_.size())
            throw NetException("NetGroup::GetClient() requested "
                               "invalid client id " + std::to_string(id));

        return connections_[id];
    }

    //! Return number of connections in this group.
    size_t Size() const
    {
        return connections_.size();
    }

    //! Return my rank in the connection group
    size_t MyRank() const
    {
        return my_rank_;
    }

    /*!
     * Sends a message (data,len) to worker dest, returns status code.
     */
    void SendMsg(ClientId dest, const void* data, size_t len)
    {
        LOG << "NetGroup::Send"
            << " " << my_rank_ << " -> " << dest
            << " data=" << hexdump(data, len)
            << " len=" << len;

        if (dest >= connections_.size())
            throw NetException("NetGroup::Send() requested "
                               "invalid client id " + std::to_string(dest));

        if (dest == my_rank_)
            throw NetException("NetGroup::Send() requested to send to self.");

        connections_[dest].SendString(data, len);
    }

    /*!
     * Sends a message to worker dest, returns status code.
     */
    void SendMsg(ClientId dest, const std::string& message)
    {
        return SendMsg(dest, message.data(), message.size());
    }

    /*!
     * Receives a message into (data,len) from worker src, returns status code.
     */
    void ReceiveFrom(ClientId src, std::string* outdata)
    {
        if (src >= connections_.size())
            throw NetException("NetGroup::ReceiveFrom() requested "
                               "invalid client id " + std::to_string(src));

        if (src == my_rank_)
            throw NetException("NetGroup::ReceiveFrom() requested to send to self.");

        LOG << "NetGroup::ReceiveFrom src=" << src;

        connections_[src].ReceiveString(outdata);

        LOG << "done NetGroup::ReceiveFrom"
            << " " << src << " -> " << my_rank_
            << " data=" << hexdump(*outdata);
    }

    /*!
     * Receives a message from any worker into (data,len), puts worker id in
     * *src, and returns status code.
     */
    void ReceiveFromAny(ClientId* out_src, std::string* out_data)
    {
        fd_set fd_set;
        int max_fd = 0;

        FD_ZERO(&fd_set);

        // add file descriptor to read set for poll TODO(ts): make this faster
        // (somewhen)

        sLOG << "--- NetGroup::ReceiveFromAny() - select():";

        for (size_t i = 0; i != connections_.size(); ++i)
        {
            if (i == my_rank_) continue;

            int fd = connections_[i].GetFileDescriptor();
            FD_SET(fd, &fd_set);
            max_fd = std::max(max_fd, fd);
            sLOG << "select from fd=" << fd;
        }

        int retval = select(max_fd + 1, &fd_set, NULL, NULL, NULL);

        if (retval < 0) {
            perror("select()");
            abort();
        }
        else if (retval == 0) {
            perror("select() TIMEOUT");
            abort();
        }

        for (size_t i = 0; i < connections_.size(); i++)
        {
            if (i == my_rank_) continue;

            int fd = connections_[i].GetFileDescriptor();

            if (FD_ISSET(fd, &fd_set))
            {
                sLOG << "select() readable fd" << fd;

                *out_src = i;
                return ReceiveFrom(i, out_data);
            }
        }

        sLOG << "Select() returned but no fd was readable.";

        return ReceiveFromAny(out_src, out_data);
    }

    void Close()
    {
        for (size_t i = 0; i != connections_.size(); ++i)
        {
            if (i == my_rank_) continue;
            connections_[i].Close();
        }
    }

protected:
    //! Construct object but initialize other fields later.
    NetGroup(ClientId id, size_t num_clients)
        : my_rank_(id),
          connections_(num_clients)
    { }

private:
    Socket listenSocket_ = Socket::Create();
    struct sockaddr_in serverAddress_;

    //! The client id of this object in the NetGroup.
    const ClientId my_rank_;

    //! Connections to all other clients in the NetGroup.
    std::vector<NetConnection> connections_;

    // int OpenConnections()
    // {
    //     struct sockaddr clientAddress;
    //     socklen_t clientAddressLen;

    //     SocketAddress sa(serverAddress_

    //     serverAddress_.sin_family = AF_INET;
    //     serverAddress_.sin_addr.s_addr = INADDR_ANY;
    //     //serverAddress_.sin_port = htons(endpoints[my_rank_].port);

    //     if (my_rank_ > 0)
    //     {
    //         if (!listenSocket_.bind(sa))
    //             return NET_SERVER_INIT_FAILED;

    //         listenSocket_.listen();

    //         //Accept connections of all hosts with lower ID.
    //         for (unsigned int i = 0; i < my_rank_; i++) {
    //             int fd = accept(serverSocket_, &clientAddress, &clientAddressLen);

    //             if (fd <= 0) {
    //                 return NET_SERVER_ACCEPT_FAILED;
    //             }
    //             //Hosts will always connect in Order.
    //             connections_[i] = new NetConnection(fd, i);
    //         }
    //     }
    //     //shutdown(serverSocket_, SHUT_WR);

    //     //Connect to all hosts with larger ID (in order).
    //     for (unsigned int i = my_rank_ + 1; i < endpoints.size(); i++) {
    //         NetConnection* client = new NetConnection(i);
    //         //Wait until server opens.
    //         int ret = 0;
    //         do {
    //             ret = client->Connect(endpoints[i].hostport);
    //             if (ret == NET_CONNECTION_CONNECT_FAILED) {
    //                 std::this_thread::sleep_for(std::chrono::milliseconds(1));
    //             }
    //             else if (ret != NET_CONNECTION_SUCCESS) {
    //                 return NET_SERVER_CONNECTION_FAILED;
    //             }
    //         } while (ret == NET_CONNECTION_CONNECT_FAILED);
    //         connections_[i] = client;
    //     }

    //     connections_[my_rank_] = NULL;

    //     //We finished connecting. :)
    //     return NET_SERVER_SUCCESS;
    // }
};

} // namespace c7a

#endif // !C7A_COMMUNICATION_NET_GROUP_HEADER

/******************************************************************************/
