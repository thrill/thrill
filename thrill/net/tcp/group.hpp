/*******************************************************************************
 * thrill/net/tcp/group.hpp
 *
 * net::Group is a collection of NetConnections providing simple MPI-like
 * collectives and point-to-point communication.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_TCP_GROUP_HEADER
#define THRILL_NET_TCP_GROUP_HEADER

#include <thrill/common/logger.hpp>
#include <thrill/net/group.hpp>
#include <thrill/net/tcp/connection.hpp>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

namespace thrill {
namespace net {
namespace tcp {

//! \addtogroup net Network Communication
//! \{

/*!
 * Collection of NetConnections to workers, allows point-to-point client
 * communication and simple collectives like MPI.
 */
class Group final : public net::Group
{
    static const bool debug = false;

    using TcpConnection = tcp::Connection;

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
    TcpConnection & tcp_connection(size_t id) {
        if (id >= connections_.size())
            throw Exception("Group::Connection() requested "
                            "invalid client id " + std::to_string(id));

        if (id == my_rank_)
            throw Exception("Group::Connection() requested "
                            "connection to self.");

        // return Connection to client id.
        return connections_[id];
    }

    net::Connection & connection(size_t id) final {
        return tcp_connection(id);
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

private:
    bool connected_ = false;

    //! Connections to all other clients in the Group.
    std::vector<TcpConnection> connections_;
};

//! \}

} // namespace tcp
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_TCP_GROUP_HEADER

/******************************************************************************/
