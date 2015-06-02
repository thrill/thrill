/*******************************************************************************
 * c7a/net/net_group.hpp
 *
 * net::Group is a collection of NetConnections providing simple MPI-like
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

//TODO(ej) Cleanup the Group. Make to a sole collection holding a bunch of connections.
//Move everything else into appropriate channel.

/*!
 * Collection of NetConnections to workers, allows point-to-point client
 * communication and simple collectives like MPI.
 */
class Group
{
    static const bool debug = false;

public:
    //! \name Construction and Initialization
    //! \{

    //! Construct a mock Group using a complete graph of local stream sockets
    //! for testing, and starts a thread for each client, which gets passed the
    //! Group object. This is ideal for testing network communication
    //! protocols. See tests/net/test-net-group.cpp for examples.
    //! @param num_clients The number of clients to spawn.
    //! @param thread_function The function to execute for each client.
    static void ExecuteLocalMock(
        size_t num_clients,
        const std::function<void(Group*)>& thread_function);

    //! Default empty constructor, must be Initialize()d later.
    Group()
    { }

    //! Initialize a real Group for construction from the NetManager.
    void Initialize(ClientId my_rank, size_t group_size) {
        assert(my_rank_ == -1u);
        my_rank_ = my_rank;
        connections_.resize(group_size);
    }

    //! Initializing constructor, used by tests for creating Groups.
    Group(ClientId my_rank, size_t group_size) {
        Initialize(my_rank, group_size);
    }

    //! \}

    //! non-copyable: delete copy-constructor
    Group(const Group&) = delete;
    //! non-copyable: delete assignment operator
    Group& operator = (const Group&) = delete;

    //! \name Status und Access to NetConnections
    //! \{

    //! Return Connection to client id.
    Connection & connection(ClientId id) {
        if (id >= connections_.size())
            throw Exception("Group::Connection() requested "
                            "invalid client id " + std::to_string(id));

        if (id == my_rank_)
            throw Exception("Group::Connection() requested "
                            "connection to self.");

        return connections_[id];
    }       //! Return Connection to client id.

    /**
     * @brief Assigns a connection to this net group.
     * @details This method swaps the net connection to memory managed by this group.
     *          The reference given to that method will be invalid afterwards.
     *
     * @param connection The connection to assign.
     * @return A ref to the assigned connection, which is always valid, but might be different from the
     * inut connection.
     */
    Connection & AssignConnection(Connection& connection) {
        if (connection.peer_id() >= connections_.size())
            throw Exception("Group::GetClient() requested "
                            "invalid client id "
                            + std::to_string(connection.peer_id()));

        connections_[connection.peer_id()] = std::move(connection);

        return connections_[connection.peer_id()];
    }

    //! Return number of connections in this group.
    size_t Size() const {
        return connections_.size();
    }

    //! Return my rank in the connection group
    size_t MyRank() const {
        return my_rank_;
    }

    //! Closes all client connections
    void Close() {
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
    ~Group() {
        Close();
    }

    //! \}

    //! \name Richer ReceiveFromAny Functions
    //! \{

    /*!
     * Receive a fixed-length integral type from any worker into out_value, puts
     * worker id in *src.
     *
     * @param out_src The id of the client the data was received from.
     * @param out_value The received value.
     */
    template <typename T>
    void ReceiveFromAny(ClientId* out_src, T* out_value) {
        fd_set fd_set;
        int max_fd = 0;

        FD_ZERO(&fd_set);

        // TODO(ej) use NetDispatcher here?
        // add file descriptor to read set for poll TODO(ts): make this faster
        // (somewhen)

        sLOG0 << "--- Group::ReceiveFromAny() - select():";

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
     *
     * @param out_src The id of the worker the string was received from.
     * @param out_data The string received from the worker.
     */
    void ReceiveStringFromAny(ClientId* out_src, std::string* out_data) {
        fd_set fd_set;
        int max_fd = 0;

        FD_ZERO(&fd_set);

        // add file descriptor to read set for poll TODO(ts): make this faster
        // (somewhen)

        sLOG0 << "--- Group::ReceiveFromAny() - select():";

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

    /**
     * @brief Sends a string to a worker.
     * @details Sends a string to a worker.
     *
     * @param dest The worker to send the string to.
     * @param data The string to send.
     */
    void SendStringTo(ClientId dest, const std::string& data) {
        this->connection(dest).SendString(data);
    }

    /**
     * @brief Receives a string from the given worker.
     * @details Receives a string from the given worker.
     *
     * @param src The worker to receive the string from.
     * @param data A pointer to the string where the received string should be stored.
     */
    void ReceiveStringFrom(ClientId src, std::string* data) {
        this->connection(src).ReceiveString(data);
    }

    /**
     * @brief Sends a fixed lentgh type to the given worker.
     * @details Sends a fixed lentgh type to the given worker.
     *
     * @param dest The worker to send the data to.
     * @param data The data to send.
     */
    template <typename T>
    void SendTo(ClientId dest, const T& data) {
        this->connection(dest).Send(data);
    }

    /**
     * @brief Receives a fixed length type from the given worker.
     * @details Receives a fixed length type from the given worker.
     *
     * @param src The worker to receive the fixed length type from.
     * @param data A pointer to the location where the received data should be stored.
     */
    template <typename T>
    void ReceiveFrom(ClientId src, T* data) {
        this->connection(src).Receive(data);
    }

    /**
     * @brief Broadcasts a string to all workers.
     * @details Broadcasts a string to all workers.
     *
     * @param data The string to broadcast.
     */
    void BroadcastString(const std::string& data) {
        for (size_t i = 0; i < connections_.size(); i++)
        {
            if (i == my_rank_) continue;
            SendStringTo(i, data);
        }
    }

    /**
     * @brief Broadcasts a fixed length type to all workers.
     * @details Broadcasts a fixed length type to all workers.
     *
     * @param data The data to broadcast.
     */
    template <typename T>
    void BroadcastString(const T& data) {
        for (size_t i = 0; i < connections_.size(); i++)
        {
            if (i == my_rank_) continue;
            SendStringTo(i, data);
        }
    }

    //! \}

    //! \name Collective Operations
    //! \{

    //TODO(rh) Please migrate into a different class, since Group is an abstract concept that bundles connections.

    template <typename T, typename BinarySumOp = common::SumOp<T> >
    void AllReduce(T& value, BinarySumOp sumOp = BinarySumOp());

    template <typename T, typename BinarySumOp = common::SumOp<T> >
    void PrefixSum(T& value, BinarySumOp sumOp = BinarySumOp());

    template <typename T, typename BinarySumOp = common::SumOp<T> >
    void ReduceToRoot(T& value, BinarySumOp sumOp = BinarySumOp());

    template <typename T>
    void Broadcast(T& value);

    //! \}

private:
    //! The client id of this object in the Group.
    ClientId my_rank_ = -1;

    //! Connections to all other clients in the Group.
    std::vector<Connection> connections_;

    //! Socket on which to listen for incoming connections.
    Connection listener_;
};

template <typename T, typename BinarySumOp>
void Group::PrefixSum(T& value, BinarySumOp sumOp) {
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
void Group::ReduceToRoot(T& value, BinarySumOp sumOp) {
    bool active = true;
    for (size_t d = 1; d < Size(); d <<= 1) {
        if (active) {
            if (MyRank() & d) {
                connection(MyRank() - d).Send(value);
                active = false;
            }
            else if (MyRank() + d < Size()) {
                T recv_data;
                connection(MyRank() + d).Receive(&recv_data);
                value = sumOp(value, recv_data);
            }
        }
    }
}

//! Binomial-broadcasts the value of the worker with index 0 to all the others
template <typename T>
void Group::Broadcast(T& value) {
    if (MyRank() > 0) {
        ClientId from;
        ReceiveFromAny(&from, &value);
    }
    for (size_t d = 1, i = 0; ((MyRank() >> i) & 1) == 0 && d < Size(); d <<= 1, ++i) {
        if (MyRank() + d < Size()) {
            connection(MyRank() + d).Send(value);
        }
    }
}

//! Perform an All-Reduce on the workers by aggregating all values and sending
//! them backto all workers
template <typename T, typename BinarySumOp>
void Group::AllReduce(T& value, BinarySumOp sum_op) {
    ReduceToRoot(value, sum_op);
    Broadcast(value);
}

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_NET_GROUP_HEADER

/******************************************************************************/
