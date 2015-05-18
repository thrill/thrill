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
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_NET_GROUP_HEADER
#define C7A_NET_NET_GROUP_HEADER

#include <c7a/net/net_endpoint.hpp>
#include <c7a/net/net_connection.hpp>
#include <c7a/common/functional.hpp>
#include <c7a/common/logger.hpp>

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

/*!
 * Collection of NetConnections to workers, allow point-to-point client
 * communication and simple collectives like MPI.
 */
class NetGroup
{
    static const bool debug = false;

public:
    //! \name Construction
    //! \{

    //! Construct a mock NetGroup using a complete graph of local stream sockets
    //! for testing, and starts a thread for each client, which gets passed the
    //! NetGroup object. This is ideal for testing network communication
    //! protocols. See tests/net/test-net-group.cpp for examples.
    static void ExecuteLocalMock(
        size_t num_clients,
        const std::function<void(NetGroup*)>& thread_function);

    //! Construct a remote NetGroup using a list of NetEndpoints of which this
    //! object will be the my_rank-th item. The NetGroup opens a listening port
    //! on the my_rank-th NetEndpoint, which hence must be local. If
    //! construction or any connection fails, a NetException is thrown.
    NetGroup(ClientId my_rank,
             size_t group_size);
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
            throw Exception("NetGroup::Connection() requested "
                            "invalid client id " + std::to_string(id));

        if (id == my_rank_)
            throw Exception("NetGroup::Connection() requested "
                            "connection to self.");

        return connections_[id];
    }       //! Return NetConnection to client id.

    void SetConnection(ClientId id, NetConnection& connection)
    {
        if (id >= connections_.size())
            throw Exception("NetGroup::GetClient() requested "
                            "invalid client id " + std::to_string(id));

        std::swap(connections_[id], connection);
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
        my_rank_ = -1;
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

    template <typename T, typename BinarySumOp = SumOp<T> >
    void PrefixSum(T& value, BinarySumOp sumOp = BinarySumOp());

    template <typename T, typename BinarySumOp = SumOp<T> >
    void ReduceToRoot(T& value, BinarySumOp sumOp = BinarySumOp());

    template <typename T>
    void Broadcast(T value);

    //! \}

private:
    //! The client id of this object in the NetGroup.
    ClientId my_rank_;

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

    for (size_t d = 1; d < Size(); d <<= 1)
    {
        // Send value to worker with id = id XOR d
        if ((MyRank() ^ d) < Size()) {
            Connection(MyRank() ^ d).Send(value);
            sLOG << "ALL_REDUCE: Worker" << MyRank() << ": Sending" << value
                 << "to worker" << (MyRank() ^ d);
        }

        // Receive value from worker with id = id XOR d
        T recv_data;
        if ((MyRank() ^ d) < Size()) {
            Connection(MyRank() ^ d).Receive(&recv_data);
            value = sum_op(value, recv_data);
            sLOG << "ALL_REDUCE: Worker" << MyRank() << ": Received" << recv_data
                 << "from worker" << (MyRank() ^ d)
                 << "value =" << value;
        }
    }

    sLOG << "ALL_REDUCE: Worker" << MyRank()
         << ": value after all reduce =" << value;
}

template <typename T, typename BinarySumOp>
void NetGroup::PrefixSum(T& value, BinarySumOp sumOp)
{
    // The total sum in the current hypercube. This is stored, because later,
    // bigger hypercubes need this value.
    T total_sum = value;

    for (size_t d = 1; d < Size(); d <<= 1)
    {
        // Send total sum of this hypercube to worker with id = id XOR d
        if ((MyRank() ^ d) < Size()) {
            Connection(MyRank() ^ d).Send(total_sum);
            sLOG << "PREFIX_SUM: Worker" << MyRank() << ": Sending" << total_sum
                 << "to worker" << (MyRank() ^ d);
        }

        // Receive total sum of smaller hypercube from worker with id = id XOR d
        T recv_data;
        if ((MyRank() ^ d) < Size()) {
            Connection(MyRank() ^ d).Receive(&recv_data);
            total_sum = sumOp(total_sum, recv_data);
            // Variable 'value' represents the prefix sum of this worker
            if (MyRank() & d)
                value = sumOp(value, recv_data);
            sLOG << "PREFIX_SUM: Worker" << MyRank() << ": Received" << recv_data
                 << "from worker" << (MyRank() ^ d)
                 << "value =" << value;
        }
    }

    sLOG << "PREFIX_SUM: Worker" << MyRank()
         << ": value after prefix sum =" << value;
}

//! Perform a binomial tree reduce to the worker with index 0
template <typename T, typename BinarySumOp>
void NetGroup::ReduceToRoot(T& value, BinarySumOp sumOp)
{
    bool active = true;
    for (size_t d = 1; d < Size(); d <<= 1) {
        if (active) {
            if (MyRank() & d) {
                Connection(MyRank() - d).Send(value);
                active = false;
            } else if (MyRank() + d < Size()) {
                T recv_data;
                Connection(MyRank() + d).Receive(&recv_data);
                value = sumOp(value, recv_data);
            }
        }
    }
}

//! Binomial-broadcasts the value of the worker with index 0 to all the others
template <typename T>
void NetGroup::Broadcast(T value)
{   
    sLOG << "I'm in NetGroup::Broadcast:369";
    if (MyRank() > 0) {
        ClientId from;
        ReceiveFromAny(&from, &value);
    }
    for (size_t d = 1, i = 0; ((MyRank() >> i) & 1) == 0; d <<= 1, ++i) {
        if (MyRank() + d < Size()) {
            Connection(MyRank() + d).Send(value);
        }
    }
    sLOG << "Worker" << MyRank() << ":" << value;
}

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_NET_GROUP_HEADER

/******************************************************************************/
