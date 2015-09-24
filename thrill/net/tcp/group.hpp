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
 * This file has no license. Only Chuck Norris can compile it.
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

//! \addtogroup net_tcp TCP Socket API
//! \{

/*!
 * Collection of NetConnections to workers, allows point-to-point client
 * communication and simple collectives like MPI.
 */
class Group final : public net::Group
{
    static const bool debug = false;

public:
    //! \name Construction and Initialization
    //! \{

    /*!
     * Construct a test network with an underlying full mesh of local loopback
     * stream sockets for testing. Returns vector of net::Group interfaces for
     * each virtual client. This is ideal for testing network communication
     * protocols.
     */
    static std::vector<std::unique_ptr<Group> > ConstructLoopbackMesh(
        size_t num_hosts);

    /*!
     * Construct a test network with an underlying full mesh of _REAL_ tcp
     * streams interconnected via localhost ports.
     */
    static std::vector<std::unique_ptr<Group> > ConstructLocalRealTCPMesh(
        size_t num_hosts);

    //! Initializing constructor, used by tests for creating Groups.
    Group(size_t my_rank, size_t group_size)
        : net::Group(my_rank),
          connections_(group_size) { }

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
    Connection & tcp_connection(size_t id) {
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

    mem::mm_unique_ptr<Dispatcher> ConstructDispatcher(
        mem::Manager& mem_manager) const final;

    /**
     * \brief Assigns a connection to this net group.
     * \details This method swaps the net connection to memory managed by this group.
     *          The reference given to that method will be invalid afterwards.
     *
     * \param connection The connection to assign.
     *
     * \return A ref to the assigned connection, which is always valid, but
     * might be different from the inut connection.
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
    }

    //! Closes all client connections
    ~Group() {
        Close();
    }

    //! \}

private:
    //! Connections to all other clients in the Group.
    std::vector<Connection> connections_;
};

//! \}

} // namespace tcp
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_TCP_GROUP_HEADER

/******************************************************************************/
