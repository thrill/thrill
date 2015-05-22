/*******************************************************************************
 * c7a/net/net_manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_NET_MANAGER_HEADER
#define C7A_NET_NET_MANAGER_HEADER

#include <c7a/net/net_endpoint.hpp>
#include <c7a/net/net_connection.hpp>
#include <c7a/net/net_dispatcher.hpp>
#include <c7a/net/net_group.hpp>

#include <vector>
#include <functional>

namespace c7a {
namespace net {

/**
 * @brief Manages communication.
 * @details Manages communication and handles errors.
 */
class NetManager
{
    static const bool debug = false;

public:
    static const size_t kGroupCount = 3;

private:
    NetGroup groups_[kGroupCount];
    NetConnection listener_;
    ClientId my_rank_;
    NetDispatcher dispatcher_;

    typedef lowlevel::Socket Socket;
    typedef lowlevel::SocketAddress SocketAddress;

    //! Array of opened connections that are not assigned to any (group,id)
    //! client, yet. This must be a deque. When welcomes are received the
    //! NetConnection is moved out of the deque into the right NetGroup.
    std::deque<NetConnection> connections_;

    /**
     * @brief Converts a c7a endpoint list into a list of socket address.
     *
     * @param endpoints The endpoint list to convert.
     * @return The socket addresses to use internally.
     */
    std::vector<SocketAddress> GetAddressList(
        const std::vector<NetEndpoint>& endpoints) {
        std::vector<SocketAddress> addressList;

        for (const NetEndpoint& ne : endpoints)
        {
            addressList.push_back(SocketAddress(ne.hostport));
            if (!addressList.back().IsValid()) {
                throw Exception(
                          "Error resolving NetEndpoint " + ne.hostport
                          + ": " + addressList.back().GetResolveError());
            }
        }

        return addressList;
    }

    struct WelcomeMsg
    {
        uint32_t c7a;
        uint32_t group_id;
        ClientId id;
    };

    static const uint32_t c7a_sign = 0x0C7A0C7A;

    bool InitializationFinished(size_t endpointCount) {
        for (uint32_t g = 0; g < kGroupCount; g++) {
            for (ClientId id = 0; id < groups_[g].Size(); ++id) {
                if (id == my_rank_) continue;

                if (groups_[g].Connection(id).state()
                    != ConnectionState::Connected)
                    return false;
            }
        }

        return true;
    }

public:
    /**
     * @brief Spawns theads for each NetGroup and calls the given thread
     * function for each client to simulate.
     */
    static void ExecuteLocalMock(
        size_t num_clients,
        const std::function<void(NetGroup*)>& systemThreadFunction,
        const std::function<void(NetGroup*)>& flowThreadFunction,
        const std::function<void(NetGroup*)>& dataThreadFunction) {
        // Adjust this method too if groupcount is different
        die_unless(kGroupCount == 3);

        std::vector<std::thread*> threads(kGroupCount);

        threads[0] = new std::thread(
            [=] {
                NetGroup::ExecuteLocalMock(num_clients, systemThreadFunction);
            });
        threads[1] = new std::thread(
            [=] {
                NetGroup::ExecuteLocalMock(num_clients, flowThreadFunction);
            });
        threads[2] = new std::thread(
            [=] {
                NetGroup::ExecuteLocalMock(num_clients, dataThreadFunction);
            });

        for (size_t i = 0; i != threads.size(); ++i) {
            threads[i]->join();
            delete threads[i];
        }
    }

    void Initialize(size_t my_rank, const std::vector<NetEndpoint>& endpoints) {
        my_rank_ = my_rank;

        if (connections_.size() != 0) {
            throw new Exception("This net manager has already been initialized.");
        }

        for (size_t i = 0; i < kGroupCount; i++) {
            groups_[i].Initialize(my_rank_, endpoints.size());
        }

        die_unless(my_rank_ < endpoints.size());

        std::vector<SocketAddress> addressList
            = GetAddressList(endpoints);

        {
            Socket listen_socket = Socket::Create();
            listen_socket.SetReuseAddr();

            const SocketAddress& lsa = addressList[my_rank_];

            if (listen_socket.bind(lsa) != 0)
                throw Exception("Could not bind listen socket to "
                                + lsa.ToStringHostPort(), errno);

            if (listen_socket.listen() != 0)
                throw Exception("Could not listen on socket "
                                + lsa.ToStringHostPort(), errno);

            listener_ = std::move(NetConnection(listen_socket));
        }

        //TODO ej - remove when Connect(...) gets really async.
        sleep(1);
        // initiate connections to all hosts with higher id.

        for (uint32_t g = 0; g < kGroupCount; g++) {
            for (ClientId id = my_rank_ + 1; id < addressList.size(); ++id) {
                AsyncConnect(g, id, addressList[id]);
            }
        }

        dispatcher_.AddRead(listener_,
                            [=](NetConnection& nc) {
                                return OnIncomingConnection(nc);
                            });

        while (!InitializationFinished(endpoints.size()))
        {
            LOG << "Client " << my_rank_ << " dispatching.";
            dispatcher_.Dispatch();
        }

        //Could dispose listen connection here.
        listener_.Close();

        LOG << "Client " << my_rank_ << " done";

        for (uint32_t j = 0; j < kGroupCount; j++) {
            // output list of file descriptors connected to partners
            for (size_t i = 0; i != addressList.size(); ++i) {
                if (i == my_rank_) continue;
                LOG << "NetGroup " << j << " link " << my_rank_ << " -> " << i << " = fd "
                    << groups_[j].Connection(i).GetSocket().fd();

                // TODO(tb): temporarily turn all fds back to blocking, till the
                // whole asio schema works.
                groups_[j].Connection(i).GetSocket().SetNonBlocking(false);
            }
        }
    }

    /*!
     * Initiate new async connect() to client (group,id) at given address
     */
    void AsyncConnect(
        uint32_t group, size_t id, SocketAddress& address) {
        // construct a new socket (old one is destroyed)
        NetConnection& nc = groups_[group].Connection(id);
        if (nc.IsValid()) nc.Close();

        nc = std::move(NetConnection(Socket::Create()));
        nc.set_group_id(group);
        nc.set_peer_id(id);

        // start async connect
        nc.GetSocket().SetNonBlocking(true);
        int res = nc.GetSocket().connect(address);

        nc.set_state(ConnectionState::Connecting);

        if (res == 0) {
            LOG << "Early connect success. This should not happen.";
            // connect() already successful? this should not be.
            OnConnected(nc, group, id, address);
        }
        else if (errno == EINPROGRESS) {
            // connect is in progress, will wait for completion.
            dispatcher_.AddWrite(nc, [=](NetConnection& nc) {
                                     return OnConnected(nc, group, id, address);
                                 });
        }
        else {
            // Failed to even try the connection - this might be a permanent
            // error.
            nc.set_state(ConnectionState::Invalid);

            throw Exception("Error connecting to client "
                            + std::to_string(nc.peer_id()) + " via "
                            + address.ToStringHostPort(), errno);
        }
    }

    void OnHelloSent(NetConnection& conn) {
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
     * @details
     * @return
     */
    bool OnConnected(
        NetConnection& conn, uint32_t group, size_t id, SocketAddress address) {
        int err = conn.GetSocket().GetError();

        if (err != 0) {
            conn.set_state(ConnectionState::Disconnected);
            //Try a reconnect
            //TODO(ej): Figure out if we need a timer here.
            LOG1 << "Error connecting.";
            //std::this_thread::sleep_for(std::chrono::milliseconds(100));
            //Connect(conn, address);
            return false;
        }

        die_unless(conn.GetSocket().IsValid());

        conn.set_state(ConnectionState::TransportConnected);

        LOG << "OnConnected() " << my_rank_ << " connected"
            << " fd=" << conn.GetSocket().fd()
            << " to=" << conn.GetSocket().GetPeerAddress()
            << " group=" << conn.group_id();

        // send welcome message
        const WelcomeMsg hello = { c7a_sign, group, my_rank_ };

        dispatcher_.AsyncWriteCopy(conn, &hello, sizeof(hello),
                                   [=](NetConnection& nc) {
                                       return OnHelloSent(nc);
                                   });

        LOG << "Client " << my_rank_ << " sent active hello to "
            << "client group " << group << " id " << id;

        dispatcher_.AsyncRead(conn, sizeof(hello),
                              [=](NetConnection& nc, Buffer&& b) {
                                  OnOutgoingWelcome(nc, std::move(b));
                              });

        return false;
    }

    /**
     * @brief Receives and handels a hello message.
     * @details
     *
     * @return
     */
    bool OnOutgoingWelcome(NetConnection& conn, Buffer&& buffer) {
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

    //! Receives and handles a hello message of fixed size.
    bool OnIncomingWelcomeAndReply(NetConnection& conn, Buffer&& buffer) {
        die_unless(conn.GetSocket().IsValid());
        die_unless(conn.state() != ConnectionState::TransportConnected);

        const WelcomeMsg* msg_in = reinterpret_cast<const WelcomeMsg*>(buffer.data());
        die_unequal(msg_in->c7a, c7a_sign);

        LOG << "client " << my_rank_ << " got signature from client"
            << " group " << msg_in->group_id
            << " id " << msg_in->id;

        die_unless(msg_in->group_id < kGroupCount);
        die_unless(msg_in->id < groups_[msg_in->group_id].Size());

        die_unequal(groups_[msg_in->group_id].Connection(msg_in->id).state(),
                    ConnectionState::Invalid);

        // move connection into NetGroup.

        conn.set_state(ConnectionState::HelloReceived);
        conn.set_peer_id(msg_in->id);
        conn.set_group_id(msg_in->group_id);

        NetConnection& c = groups_[msg_in->group_id].AssignConnection(conn);

        // send welcome message (via new connection's place)

        const WelcomeMsg msg_out = { c7a_sign, msg_in->group_id, my_rank_ };

        dispatcher_.AsyncWriteCopy(c, &msg_out, sizeof(msg_out),
                                   [=](NetConnection& nc) {
                                       return OnHelloSent(nc);
                                   });

        LOG << "Client " << my_rank_
            << " sent passive hello to client " << msg_in->id;

        return false;
    }

    //! Signal when the listening socket has a new connection: wait for welcome
    //! message.
    bool OnIncomingConnection(NetConnection& conn) {
        // accept listening socket
        connections_.emplace_back(conn.GetSocket().accept());
        die_unless(connections_.back().GetSocket().IsValid());

        conn.set_state(ConnectionState::TransportConnected);

        LOG << "OnIncomingConnection() " << my_rank_ << " accepted connection"
            << " fd=" << connections_.back().GetSocket().fd()
            << " from=" << connections_.back().GetPeerAddress();

        // wait for welcome message from other side
        dispatcher_.AsyncRead(connections_.back(), sizeof(WelcomeMsg),
                              [&](NetConnection& nc, Buffer&& b) {
                                  OnIncomingWelcomeAndReply(nc, std::move(b));
                              });

        // wait for more connections.
        return true;
    }

    NetGroup & GetSystemNetGroup() {
        return groups_[0];
    }

    NetGroup & GetFlowNetGroup() {
        return groups_[1];
    }

    NetGroup & GetDataNetGroup() {
        return groups_[2];
    }
};

} // namespace net
} // namespace c7a

#endif // !C7A_NET_NET_MANAGER_HEADER

/******************************************************************************/
