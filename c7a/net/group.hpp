/*******************************************************************************
 * c7a/net/group.hpp
 *
 * net::Group is a collection of NetConnections providing simple MPI-like
 * collectives and point-to-point communication.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_GROUP_HEADER
#define C7A_NET_GROUP_HEADER

#include <c7a/common/logger.hpp>
#include <c7a/net/connection.hpp>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

namespace c7a {
namespace net {

//! \addtogroup net Network Communication
//! \{

// TODO(ej) Cleanup the Group. Make to a sole collection holding a bunch of connections.
// Move everything else into appropriate channel.

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

    /*!
     * Construct a mock Group vector with an underlying full mesh of local
     * stream sockets for testing. Returns vector of net::Group interfaces for
     * each virtual client. This is ideal for testing network communication
     * protocols. See tests/net/test-net-group.cpp for examples.
     *
     * @param num_clients The number of clients in the mesh.
     */
    static std::vector<Group> ConstructLocalMesh(size_t num_clients);

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
    void Initialize(size_t my_rank, size_t group_size) {
        my_rank_ = my_rank;
        assert(!connected_);
        connected_ = true;
        connections_.resize(group_size);
    }

    //! Initializing constructor, used by tests for creating Groups.
    Group(size_t my_rank, size_t group_size) {
        Initialize(my_rank, group_size);
    }

    //! \}

    //! non-copyable: delete copy-constructor
    Group(const Group&) = delete;
    //! non-copyable: delete assignment operator
    Group& operator = (const Group&) = delete;
    //! move-constructor: default
    Group(Group&&) = default;
    //! move-assignment operator: default
    Group& operator = (Group&&) = default;

    //! \name Status and Access to NetConnections
    //! \{

    //! Return Connection to client id.
    Connection & connection(size_t id) {
        if (id >= connections_.size())
            throw Exception("Group::Connection() requested "
                            "invalid client id " + std::to_string(id));

        if (id == my_rank_)
            throw Exception("Group::Connection() requested "
                            "connection to self.");

        // return Connection to client id.
        return connections_[id];
    }

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

    //! Return number of connections in this group (= number computing hosts)
    size_t num_hosts() const {
        return connections_.size();
    }

    //! Return my rank in the connection group (computing hosts)
    size_t my_host_rank() const {
        return my_rank_;
    }

    //! Closes all client connections
    void Close() {
        for (size_t i = 0; i != connections_.size(); ++i)
        {
            if (i == my_rank_) continue;

            if (connections_[i].IsValid())
                connections_[i].Close();
        }

        connections_.clear();
        connected_ = false;
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
    void ReceiveFromAny(size_t* out_src, T* out_value) {
        fd_set fd_set;
        int max_fd = 0;

        FD_ZERO(&fd_set);

        // TODO(ej) use NetDispatcher here?
        // TODO(rh) use NetDispatcher here and everywhere else in this class
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
    void ReceiveStringFromAny(size_t* out_src, std::string* out_data) {
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
    void SendStringTo(size_t dest, const std::string& data) {
        connection(dest).SendString(data);
    }

    /**
     * @brief Receives a string from the given worker.
     * @details Receives a string from the given worker.
     *
     * @param src The worker to receive the string from.
     * @param data A pointer to the string where the received string should be stored.
     */
    void ReceiveStringFrom(size_t src, std::string* data) {
        connection(src).ReceiveString(data);
    }

    /**
     * @brief Sends a fixed lentgh type to the given worker.
     * @details Sends a fixed lentgh type to the given worker.
     *
     * @param dest The worker to send the data to.
     * @param data The data to send.
     */
    template <typename T>
    void SendTo(size_t dest, const T& data) {
        connection(dest).Send(data);
    }

    /**
     * @brief Receives a fixed length type from the given worker.
     * @details Receives a fixed length type from the given worker.
     *
     * @param src The worker to receive the fixed length type from.
     * @param data A pointer to the location where the received data should be stored.
     */
    template <typename T>
    void ReceiveFrom(size_t src, T* data) {
        connection(src).Receive(data);
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

private:
    //! The client id of this object in the Group.
    size_t my_rank_;

    bool connected_ = false;

    //! Connections to all other clients in the Group.
    std::vector<Connection> connections_;
};

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_GROUP_HEADER

/******************************************************************************/
