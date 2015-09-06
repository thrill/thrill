/*******************************************************************************
 * thrill/net/group.hpp
 *
 * net::Group is a collection of NetConnections providing simple MPI-like
 * collectives and point-to-point communication.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_GROUP_HEADER
#define THRILL_NET_GROUP_HEADER

#include <thrill/common/logger.hpp>
#include <thrill/net/connection.hpp>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

class GroupBase
{
public:
    // default constructor
    GroupBase() = default;

    //! non-copyable: delete copy-constructor
    GroupBase(const GroupBase&) = delete;
    //! non-copyable: delete assignment operator
    GroupBase& operator = (const GroupBase&) = delete;
    //! move-constructor: default
    GroupBase(GroupBase&&) = default;
    //! move-assignment operator: default
    GroupBase& operator = (GroupBase&&) = default;

    //! Return our rank among hosts in this group.
    size_t my_host_rank() const { return my_rank_; }

    //! Return number of connections in this group (= number computing hosts)
    virtual size_t num_hosts() const = 0;

    //! our rank in the network group
    size_t my_rank_;
};

/*!
 * Collection of NetConnections to workers, allows point-to-point client
 * communication and simple collectives like MPI.
 */
class Group : public GroupBase
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
     * \param num_clients The number of clients in the mesh.
     */
    static std::vector<Group> ConstructLocalMesh(size_t num_clients);

    //! Construct a mock Group using a complete graph of local stream sockets
    //! for testing, and starts a thread for each client, which gets passed the
    //! Group object. This is ideal for testing network communication
    //! protocols. See tests/net/test-net-group.cpp for examples.
    //! \param num_clients The number of clients to spawn.
    //! \param thread_function The function to execute for each client.
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
    TcpConnection & connection(size_t id) {
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
     * \brief Assigns a connection to this net group.
     * \details This method swaps the net connection to memory managed by this group.
     *          The reference given to that method will be invalid afterwards.
     *
     * \param connection The connection to assign.
     * \return A ref to the assigned connection, which is always valid, but might be different from the
     * inut connection.
     */
    TcpConnection & AssignConnection(TcpConnection& connection) {
        if (connection.peer_id() >= connections_.size())
            throw Exception("Group::GetClient() requested "
                            "invalid client id "
                            + std::to_string(connection.peer_id()));

        connections_[connection.peer_id()] = std::move(connection);

        return connections_[connection.peer_id()];
    }

    //! Return number of connections in this group (= number computing hosts)
    size_t num_hosts() const final {
        return connections_.size();
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

    /**
     * \brief Sends a fixed lentgh type to the given worker.
     * \details Sends a fixed lentgh type to the given worker.
     *
     * \param dest The worker to send the data to.
     * \param data The data to send.
     */
    template <typename T>
    void SendTo(size_t dest, const T& data) {
        connection(dest).Send(data);
    }

    /**
     * \brief Receives a fixed length type from the given worker.
     * \details Receives a fixed length type from the given worker.
     *
     * \param src The worker to receive the fixed length type from.
     * \param data A pointer to the location where the received data should be stored.
     */
    template <typename T>
    void ReceiveFrom(size_t src, T* data) {
        connection(src).Receive(data);
    }

    /**
     * \brief Sends a string to a worker.
     * \details Sends a string to a worker.
     *
     * \param dest The worker to send the string to.
     * \param data The string to send.
     */
    void SendStringTo(size_t dest, const std::string& data) {
        connection(dest).SendString(data);
    }

    /**
     * \brief Receives a string from the given worker.
     * \details Receives a string from the given worker.
     *
     * \param src The worker to receive the string from.
     * \param data A pointer to the string where the received string should be stored.
     */
    void ReceiveStringFrom(size_t src, std::string* data) {
        connection(src).ReceiveString(data);
    }

    //! \}

private:
    bool connected_ = false;

    //! Connections to all other clients in the Group.
    std::vector<TcpConnection> connections_;
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_GROUP_HEADER

/******************************************************************************/
