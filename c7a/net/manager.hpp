/*******************************************************************************
 * c7a/net/manager.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_MANAGER_HEADER
#define C7A_NET_MANAGER_HEADER

#include <c7a/net/endpoint.hpp>
#include <c7a/net/connection.hpp>
#include <c7a/net/dispatcher.hpp>
#include <c7a/net/group.hpp>

#include <vector>
#include <functional>

namespace c7a {
namespace net {

/**
 * @brief Initializes communication channels, manages communication c
 * hannels and handles errors.
 * @details This class is responsible for initializing the three net::Groups
 * for the major network components, SystemControl, FlowControl and DataManagement,
 */
class Manager
{
    static const bool debug = false;

public:
    /**
     * The count of net::Groups to initialize.
     * If this value is changed, the corresponding
     * getters for the net::Groups should be changed as well.
     */
    static const size_t kGroupCount = 3;

    ClientId MyRank() {
        return GetSystemGroup().MyRank();
    }

    ClientId Size() {
        return GetSystemGroup().Size();
    }

private:
    /**
     * The Groups initialized and managed
     * by this Manager.
     */
    Group groups_[kGroupCount];

    /**
     * The Connections responsible
     * for listening to incoming connections.
     */
    Connection listener_;

    /**
     * The rank associated with the local worker.
     */
    ClientId my_rank_;

    /**
     * The dispatcher instance used by this Manager
     * to perform async operations.
     */
    Dispatcher dispatcher_;

    //Some definitions for convenience
    typedef lowlevel::Socket Socket;
    typedef lowlevel::SocketAddress SocketAddress;
    typedef lowlevel::IPv4Address IPv4Address;

    //! Array of opened connections that are not assigned to any (group,id)
    //! client, yet. This must be a deque. When welcomes are received the
    //! Connection is moved out of the deque into the right Group.
    std::deque<Connection> connections_;

    /**
     * @brief Converts a c7a endpoint list into a list of socket address.
     *
     * @param endpoints The endpoint list to convert.
     * @return The socket addresses to use internally.
     */
    std::vector<SocketAddress> GetAddressList(
        const std::vector<Endpoint>& endpoints) {

        std::vector<SocketAddress> addressList;
        for (const Endpoint& ne : endpoints)
        {
            addressList.push_back(SocketAddress(ne.hostport));
            if (!addressList.back().IsValid()) {
                throw Exception(
                          "Error resolving Endpoint " + ne.hostport
                          + ": " + addressList.back().GetResolveError());
            }
        }

        return addressList;
    }

    /**
     * @brief Represents a welcome message.
     * @details Represents a welcome message that is exchanged by Connections during
     * network initialization.
     */
    struct WelcomeMsg
    {
        /**
         * The c7a flag.
         */
        uint32_t c7a;
        /**
         * The id of the Group associated with the sending Connection.
         */
        uint32_t group_id;
        /**
         * The id of the worker associated with the sending Connection.
         */
        ClientId id;
    };

    /**
     * The c7a flag - introduced by Master Timo.
     */
    static const uint32_t c7a_sign = 0x0C7A0C7A;

    /**
     * @brief Returns wether the initialization is completed.
     *
     * @details Checkts the Groups associated with this Manager and returns true or fals wether
     * the initialization is finished.
     *
     * @return True if initialization is finished, else false.
     */
    bool IsInitializationFinished() {

        for (uint32_t g = 0; g < kGroupCount; g++) {

            for (ClientId id = 0; id < groups_[g].Size(); ++id) {
                if (id == my_rank_) continue;

                //Just checking the state works since this implicitey checks the
                //size. Unset connections have state ConnectionState::Invalid.
                if (groups_[g].connection(id).state()
                    != ConnectionState::Connected)
                    return false;
            }
        }

        return true;
    }

    /**
     * @brief Starts connecting to the net connection specified.
     * @details Starts connecting to the endpoint specified by the parameters.
     * This method executes asynchronously.
     *
     * @param nc The connection to connect.
     * @param address The address of the endpoint to connect to.
     */
    void AsyncConnect(Connection& nc, SocketAddress& address) {
        // Start asynchronous connect.
        nc.GetSocket().SetNonBlocking(true);
        int res = nc.GetSocket().connect(address);

        nc.set_state(ConnectionState::Connecting);

        if (res == 0) {
            LOG << "Early connect success. This should not happen.";
            // connect() already successful? this should not be.
            OnConnected(nc, address);
        }
        else if (errno == EINPROGRESS) {
            // connect is in progress, will wait for completion.
            dispatcher_.AddWrite(nc, [this, &address](Connection& nc) {
                                     return OnConnected(nc, address);
                                 });
        }
        else {
            // Failed to even try the connection - this might be a permanent
            // error.
            nc.set_state(ConnectionState::Invalid);

            throw Exception("Error starting async connect client "
                            + std::to_string(nc.peer_id()) + " via "
                            + address.ToStringHostPort(), errno);
        }
    }

    /**
     * @brief Starts connecting to the endpoint specified by the parameters.
     * @details Starts connecting to the endpoint specified by the parameters.
     * This method executes asynchronously.
     *
     * @param group The id of the Group to connect to.
     * @param id The id of the worker to connect to.
     * @param address The address of the endpoint to connect to.
     */
    void AsyncConnect(
        uint32_t group, size_t id, SocketAddress& address) {

        // Construct a new socket (old one is destroyed)
        Connection& nc = groups_[group].connection(id);
        if (nc.IsValid()) nc.Close();

        nc = std::move(Connection(Socket::Create()));
        nc.set_group_id(group);
        nc.set_peer_id(id);

        AsyncConnect(nc, address);
    }

    /**
     * @brief Is called whenever a hello is sent.
     * @details Is called whenever a hello is sent.
     * For outgoing connections, this is the final step in the state machine.
     *
     * @param conn The connection for which the hello is sent.
     */
    void OnHelloSent(Connection& conn) {
        if (conn.state() == ConnectionState::TransportConnected) {
            conn.set_state(ConnectionState::HelloSent);
        }
        else if (conn.state() == ConnectionState::HelloReceived) {
            conn.set_state(ConnectionState::Connected);
        }
        else {
            die("State mismatch: " + std::to_string(conn.state()));
        }
    }

    /**
     * @brief Called when a connection initiated by us succeeds.
     * @details Called when a connection initiated by us succeeds on a betwork level.
     * The c7a welcome messages still have to be exchanged.
     *
     * @param conn The connection that was connected successfully.
     * @param address The associated address. This parameter is needed in case we need to reconnect.
     *
     * @return A bool indicating wether this callback should stay registered.
     */
    bool OnConnected(Connection& conn, SocketAddress& address) {

        //First, check if everything went well.
        int err = conn.GetSocket().GetError();

        if (err == Socket::Errors::ConnectionRefused ||
            err == Socket::Errors::Timeout) {

            //Connection refused. The other workers might not be online yet.

            LOG << "Connect to " << address.ToStringHostPort() <<
                " timed out or refused. Attempting reconnect";

            // Construct a new connection since the
            // socket might not be reusable.
            Connection nc;

            nc = std::move(Connection(Socket::Create()));
            nc.set_group_id(conn.group_id());
            nc.set_peer_id(conn.peer_id());

            std::swap(conn, nc);

            AsyncConnect(conn, address);

            return false;
        }
        else if (err != 0) {
            //Other failure. Fail hard.
            conn.set_state(ConnectionState::Invalid);

            throw Exception("Error connecting asyncronously to client "
                            + std::to_string(conn.peer_id()) + " via "
                            + address.ToStringHostPort(), err);
        }

        die_unless(conn.GetSocket().IsValid());

        conn.set_state(ConnectionState::TransportConnected);

        LOG << "OnConnected() " << my_rank_ << " connected"
            << " fd=" << conn.GetSocket().fd()
            << " to=" << conn.GetSocket().GetPeerAddress()
            << " group=" << conn.group_id();

        // send welcome message
        const WelcomeMsg hello = { c7a_sign, (uint32_t)conn.group_id(), my_rank_ };

        dispatcher_.AsyncWriteCopy(conn, &hello, sizeof(hello),
                                   [=](Connection& nc) {
                                       return OnHelloSent(nc);
                                   });

        LOG << "Client " << my_rank_ << " sent active hello to "
            << "client group id " << conn.group_id();

        dispatcher_.AsyncRead(conn, sizeof(hello),
                              [=](Connection& nc, Buffer&& b) {
                                  OnIncomingWelcome(nc, std::move(b));
                              });

        return false;
    }

    /**
     * @brief Receives and handels a hello message without sending a reply.
     * @details Receives and handels a hello message without sending a reply.
     *
     * @param conn The connection the hello was received for.
     * @param buffer The buffer containing the welcome message.
     *
     * @return A boolean indicating wether this handler should stay attached.
     */
    bool OnIncomingWelcome(Connection& conn, Buffer&& buffer) {

        die_unless(conn.GetSocket().IsValid());
        die_unequal(buffer.size(), sizeof(WelcomeMsg));
        die_unequal(conn.state(), ConnectionState::HelloSent);

        const WelcomeMsg* msg
            = reinterpret_cast<const WelcomeMsg*>(buffer.data());
        die_unequal(msg->c7a, c7a_sign);
        // We already know those values since we connected actively. So, check
        // for any errors.
        die_unequal(conn.peer_id(), msg->id);
        die_unequal(conn.group_id(), msg->group_id);

        conn.set_state(ConnectionState::Connected);

        LOG << "client " << my_rank_ << " got signature "
            << "from client " << msg->id;

        return false;
    }

    /**
     * @brief Receives and handles a welcome message. Also sends a reply.
     * @details Receives and handles a welcome message. Also sends a reply.
     *
     * @param conn The connection that received the welcome message.
     * @param buffer The buffer containing the welcome message.
     *
     * @return A boolean indicating wether this handler should stay attached.
     */
    bool OnIncomingWelcomeAndReply(Connection& conn, Buffer&& buffer) {

        die_unless(conn.GetSocket().IsValid());
        die_unless(conn.state() != ConnectionState::TransportConnected);

        const WelcomeMsg* msg_in = reinterpret_cast<const WelcomeMsg*>(buffer.data());
        die_unequal(msg_in->c7a, c7a_sign);

        LOG << "client " << my_rank_ << " got signature from client"
            << " group " << msg_in->group_id
            << " id " << msg_in->id;

        die_unless(msg_in->group_id < kGroupCount);
        die_unless(msg_in->id < groups_[msg_in->group_id].Size());

        die_unequal(groups_[msg_in->group_id].connection(msg_in->id).state(),
                    ConnectionState::Invalid);

        // move connection into Group.

        conn.set_state(ConnectionState::HelloReceived);
        conn.set_peer_id(msg_in->id);
        conn.set_group_id(msg_in->group_id);

        Connection& c = groups_[msg_in->group_id].AssignConnection(conn);

        // send welcome message (via new connection's place)

        const WelcomeMsg msg_out = { c7a_sign, msg_in->group_id, my_rank_ };

        dispatcher_.AsyncWriteCopy(c, &msg_out, sizeof(msg_out),
                                   [=](Connection& nc) {
                                       return OnHelloSent(nc);
                                   });

        LOG << "Client " << my_rank_
            << " sent passive hello to client " << msg_in->id;

        return false;
    }

    /**
     * @brief Handles incoming connections.
     * @details Handles incoming connections.
     *
     * @param conn The listener connection.
     * @return A boolean indicating wether this handler should stay attached.
     */
    bool OnIncomingConnection(Connection& conn) {
        // accept listening socket
        connections_.emplace_back(conn.GetSocket().accept());
        die_unless(connections_.back().GetSocket().IsValid());

        conn.set_state(ConnectionState::TransportConnected);

        LOG << "OnIncomingConnection() " << my_rank_ << " accepted connection"
            << " fd=" << connections_.back().GetSocket().fd()
            << " from=" << connections_.back().GetPeerAddress();

        // wait for welcome message from other side
        dispatcher_.AsyncRead(connections_.back(), sizeof(WelcomeMsg),
                              [&](Connection& nc, Buffer&& b) {
                                  OnIncomingWelcomeAndReply(nc, std::move(b));
                              });

        // wait for more connections.
        return true;
    }

public:
    /**
     * @brief Initializes this Manager and initializes all Groups.
     * @details Initializes this Manager and initializes all Groups.
     * When this method returns, the network system is ready to use.
     *
     * @param my_rank_ The rank of the worker that owns this Manager.
     * @param endpoints The ordered list of all endpoints, including the local worker,
     * where the endpoint at position i corresponds to the worker with id i.
     */
    void Initialize(size_t my_rank_,
                    const std::vector<Endpoint>& endpoints) {

        die_unless(my_rank_ < endpoints.size());

        this->my_rank_ = my_rank_;

        //If we heldy any connections, do not allow a new initialization.
        if (connections_.size() != 0) {
            throw new Exception("This net manager has already been initialized.");
        }

        for (size_t i = 0; i < kGroupCount; i++) {
            groups_[i].Initialize(my_rank_, endpoints.size());
        }

        //Parse endpoints.
        std::vector<SocketAddress> addressList
            = GetAddressList(endpoints);

        //Create listening socket.
        {
            Socket listen_socket = Socket::Create();
            listen_socket.SetReuseAddr();

            const SocketAddress lsa = addressList[my_rank_];

            if (listen_socket.bind(lsa) != 0)
                throw Exception("Could not bind listen socket to "
                                + lsa.ToStringHostPort(), errno);

            if (listen_socket.listen() != 0)
                throw Exception("Could not listen on socket "
                                + lsa.ToStringHostPort(), errno);

            listener_ = std::move(Connection(listen_socket));
        }

        //Initiate connections to all hosts with higher id.
        for (uint32_t g = 0; g < kGroupCount; g++) {
            for (ClientId id = my_rank_ + 1; id < addressList.size(); ++id) {
                AsyncConnect(g, id, addressList[id]);
            }
        }

        //Add reads to the dispatcher to accept new connections.
        dispatcher_.AddRead(listener_,
                            [=](Connection& nc) {
                                return OnIncomingConnection(nc);
                            });

        //Dispatch until everything is connected.
        while (!IsInitializationFinished())
        {
            LOG << "Client " << my_rank_ << " dispatching.";
            dispatcher_.Dispatch();
        }

        //All connected, Dispose listener.
        listener_.Close();

        LOG << "Client " << my_rank_ << " done";

        for (uint32_t j = 0; j < kGroupCount; j++) {
            // output list of file descriptors connected to partners
            for (size_t i = 0; i != addressList.size(); ++i) {
                if (i == my_rank_) continue;
                LOG << "Group " << j << " link " << my_rank_ << " -> " << i << " = fd "
                    << groups_[j].connection(i).GetSocket().fd();

                // TODO(tb): temporarily turn all fds back to blocking, till the
                // whole asio schema works.
                // NOTE(ej): This should be correct? Distpatch is not going to work
                // correctly with non-blocking sockets and will default to busy waiting?
                groups_[j].connection(i).GetSocket().SetNonBlocking(false);
            }
        }
    }
    /**
     * @brief Returns the net group for the system control channel.
     */
    Group & GetSystemGroup() {
        return groups_[0];
    }

    /**
     * @brief Returns the net group for the flow control channel.
     */
    Group & GetFlowGroup() {
        return groups_[1];
    }

    /**
     * @brief Returns the net group for the data manager.
     */
    Group & GetDataGroup() {
        return groups_[2];
    }

    void Close() {
        for (size_t i = 0; i < kGroupCount; i++) {
            groups_[i].Close();
        }
    }
};

} // namespace net
} // namespace c7a

#endif // !C7A_NET_MANAGER_HEADER

/******************************************************************************/
