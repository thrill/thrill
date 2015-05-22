/*******************************************************************************
 * c7a/net/net_group.hpp
 *
 * NetGroup is a collection of NetConnections providing simple MPI-like
 * collectives and point-to-point communication.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_NET_GROUP_HEADER
#define C7A_NET_NET_GROUP_HEADER

#include <c7a/net/net_endpoint.hpp>
#include <c7a/net/net_connection.hpp>
#include <c7a/common/functional.hpp>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <map>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace c7a {
namespace net {

//! \addtogroup net Network Communication
//! \{

typedef unsigned int ClientId;

//TODO(ej) Cleanup the NetGroup. Make to a sole collection holding a bunch of connections.
//Move everything else into appropriate channel.

/*!
 * Collection of NetConnections to workers, allow point-to-point client
 * communication and simple collectives like MPI.
 */
class NetGroup
{
    static const bool debug = false;

public:
    //! \name Construction and Initialization
    //! \{

    //! Construct a mock NetGroup using a complete graph of local stream sockets
    //! for testing, and starts a thread for each client, which gets passed the
    //! NetGroup object. This is ideal for testing network communication
    //! protocols. See tests/net/test-net-group.cpp for examples.
    static void ExecuteLocalMock(
        size_t num_clients,
        const std::function<void(NetGroup*)>& thread_function);

    //! Default empty constructor, must be Initialize()d later.
    NetGroup()
    { }

    //! Initialize a real NetGroup for construction from the NetManager.
    void Initialize(ClientId my_rank, size_t group_size)
    {
        assert(my_rank_ == -1u);
        my_rank_ = my_rank;
        connections_.resize(group_size);
    }

    //! Initializing constructor, used by tests for creating NetGroups.
    NetGroup(ClientId my_rank, size_t group_size)
    {
        Initialize(my_rank, group_size);
    }

    //! \}

    //! non-copyable: delete copy-constructor
    NetGroup(const NetGroup&) = delete;
    //! non-copyable: delete assignment operator
    NetGroup& operator = (const NetGroup&) = delete;

    //! \name Status und Access to NetConnections
    //! \{

    //! Return NetConnection to client id.
    NetConnection & Connection(ClientId id)
    {
        if (id >= connections_.size())
            throw Exception("NetGroup::GetClient() requested "
                            "invalid client id " + std::to_string(id));

        return connections_[id];
    }       //! Return NetConnection to client id.

    /**
     * @brief Assigns a connection to this net group.
     * @details This method swaps the net connection to memory managed by this group.
     *          The reference given to that method will be invalid afterwards.
     *
     * @param connection
     * @return
     */
    NetConnection & AssignConnection(NetConnection& connection)
    {
        if (connection.GetPeerId() >= connections_.size())
            throw Exception("NetGroup::GetClient() requested "
                            "invalid client id " + std::to_string(connection.GetPeerId()));

        std::swap(connections_[connection.GetPeerId()], connection);

        return connections_[connection.GetPeerId()];
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

    //! Closes all client connections
    void Close()
    {
        if (listener_.IsValid())
            listener_.Close();

        for (size_t i = 0; i != connections_.size(); ++i)
        {
            if (i == my_rank_) continue;

            if (connections_[i].IsValid())
                connections_[i].Close();
        }

        connections_.clear();
        my_rank_ = -1u;
    }

    //! Closes all client connections
    ~NetGroup()
    {
        Close();
    }

    //! \}

    //! \name Richer ReceiveFromAny Functions
    //! \{

    /*!
     * Receive a fixed-length integral type from any worker into out_value, puts
     * worker id in *src.
     */

    template <typename T>
    void ReceiveFromAny(ClientId* out_src, T* out_value)
    {
        fd_set fd_set;
        int max_fd = 0;

        FD_ZERO(&fd_set);

        // add file descriptor to read set for poll TODO(ts): make this faster
        // (somewhen)

        sLOG0 << "--- NetGroup::ReceiveFromAny() - select():";

        for (size_t i = 0; i != connections_.size(); ++i)
        {
            if (i == my_rank_) continue;

            int fd = connections_[i].GetSocket().fd();
            FD_SET(fd, &fd_set);
            max_fd = std::max(max_fd, fd);
            sLOG0 << "select from fd=" << fd;
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

            int fd = connections_[i].GetSocket().fd();

            if (FD_ISSET(fd, &fd_set))
            {
                sLOG << "select() readable fd" << fd;

                *out_src = i;
                return connections_[i].Receive<T>(out_value);
            }
        }

        sLOG << "Select() returned but no fd was readable.";

        return ReceiveFromAny<T>(out_src, out_value);
    }

    /*!
     * Receives a string message from any worker into out_data, which will be
     * resized as needed, puts worker id in *src.
     */
    void ReceiveStringFromAny(ClientId* out_src, std::string* out_data)
    {
        fd_set fd_set;
        int max_fd = 0;

        FD_ZERO(&fd_set);

        // add file descriptor to read set for poll TODO(ts): make this faster
        // (somewhen)

        sLOG0 << "--- NetGroup::ReceiveFromAny() - select():";

        for (size_t i = 0; i != connections_.size(); ++i)
        {
            if (i == my_rank_) continue;

            int fd = connections_[i].GetSocket().fd();
            FD_SET(fd, &fd_set);
            max_fd = std::max(max_fd, fd);
            sLOG0 << "select from fd=" << fd;
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

            int fd = connections_[i].GetSocket().fd();

            if (FD_ISSET(fd, &fd_set))
            {
                sLOG << my_rank_ << "- select() readable fd" << fd << "id" << i;

                *out_src = i;
                return connections_[i].ReceiveString(out_data);
            }
        }

        sLOG << my_rank_ << " - Select() returned but no fd was readable.";

        return ReceiveStringFromAny(out_src, out_data);
    }

    void SendStringTo(ClientId dest, const std::string& data)
    {
        this->Connection(dest).SendString(data);
    }

    void BroadcastString(const std::string& data)
    {
        for (size_t i = 0; i < connections_.size(); i++)
        {
            if (i == my_rank_) continue;
            SendStringTo(i, data);
        }
    }

    //! \}

    //! \name Collective Operations
    //! \{

    template <typename T, typename BinarySumOp = SumOp<T> >
    void AllReduce(T& value, BinarySumOp sumOp = BinarySumOp());

    //! \}

private:
    //! The client id of this object in the NetGroup.
    ClientId my_rank_ = -1;

    //! Connections to all other clients in the NetGroup.
    std::vector<NetConnection> connections_;

    //! Socket on which to listen for incoming connections.
    NetConnection listener_;
};

template <typename T, typename BinarySumOp>
void NetGroup::AllReduce(T& value, BinarySumOp sum_op)
{
    // For each dimension of the hypercube, exchange data between workers with
    // different bits at position d

    for (size_t d = 1; d < this->Size(); d <<= 1)
    {
        // Send value to worker with id = id XOR d
        if ((this->MyRank() ^ d) < this->Size()) {
            this->Connection(this->MyRank() ^ d).Send(value);
            std::cout << "LOCAL: Worker " << this->MyRank() << ": Sending " << value
                      << " to worker " << (this->MyRank() ^ d) << "\n";
        }

        // Receive value from worker with id = id XOR d
        T recv_data;
        if ((this->MyRank() ^ d) < this->Size()) {
            this->Connection(this->MyRank() ^ d).Receive(&recv_data);
            value = sum_op(value, recv_data);
            std::cout << "LOCAL: Worker " << this->MyRank() << ": Received " << recv_data
                      << " from worker " << (this->MyRank() ^ d)
                      << " value = " << value << "\n";
        }
    }

    std::cout << "LOCAL: value after all reduce " << value << "\n";
}

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_NET_GROUP_HEADER

/******************************************************************************/
