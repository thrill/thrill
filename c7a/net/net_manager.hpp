/*******************************************************************************
 * c7a/net/net_manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
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
    NetGroup netGroups_[kGroupCount];
    lowlevel::Socket listenSocket_;
    NetConnection listenConnection_;
    size_t my_rank_;
    NetDispatcher dispatcher;
    std::vector<NetConnection> connections_;

    /**
     * @brief Converts a c7a endpoint list into a list of socket address.
     *
     * @param endpoints The endpoint list to convert.
     * @return The socket addresses to use internally.
     */
    std::vector<lowlevel::SocketAddress> GetAddressList(const std::vector<NetEndpoint>& endpoints) {
        std::vector<lowlevel::SocketAddress> addressList;

        for (const NetEndpoint& ne : endpoints)
        {
            addressList.push_back(lowlevel::SocketAddress(ne.hostport));
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
        uint32_t groupId;
        ClientId id;
    };
    static const uint32_t c7a_sign = 0x0C7A0C7A;

    bool InitializationFinished(size_t endpointCount) {
        if (connections_.size() != (endpointCount - 1) * kGroupCount)
            return false;

        for (size_t i = 0; i < connections_.size(); i++)
            if (connections_[i].GetState() != ConnectionState::Connected)
                return false;

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
        die_unless(kGroupCount == 3); //Adjust this method too if groupcount is different
        std::vector<std::thread*> threads(kGroupCount);

        threads[0] = new std::thread([=] {
                                         NetGroup::ExecuteLocalMock(num_clients, systemThreadFunction);
                                     });
        threads[1] = new std::thread([=] {
                                         NetGroup::ExecuteLocalMock(num_clients, flowThreadFunction);
                                     });
        threads[2] = new std::thread([=] {
                                         NetGroup::ExecuteLocalMock(num_clients, dataThreadFunction);
                                     });

        for (size_t i = 0; i != threads.size(); ++i) {
            threads[i]->join();
            delete threads[i];
        }
    }

    void Initialize(size_t my_rank_, const std::vector<NetEndpoint>& endpoints) {
        my_rank_ = my_rank;

        if (connections_.size() != 0) {
            throw new Exception("This net manager has already been initialized.");
        }

        connections_.reserve(endpoints.size() * kGroupCount);

        for (size_t i = 0; i < kGroupCount; i++) {
            netGroups_[i].Initialize(my_rank_, endpoints.size());
        }

        die_unless(my_rank_ < endpoints.size());

        std::vector<lowlevel::SocketAddress> addressList = GetAddressList(endpoints);

        listenSocket_ = lowlevel::Socket::Create();
        listenSocket_.SetReuseAddr();

        const lowlevel::SocketAddress& lsa = addressList[my_rank_];

        if (listenSocket_.bind(lsa) != 0)
            throw Exception("Could not bind listen socket to "
                            + lsa.ToStringHostPort(), errno);

        if (listenSocket_.listen() != 0)
            throw Exception("Could not listen on socket "
                            + lsa.ToStringHostPort(), errno);

        listenConnection_ = std::move(NetConnection(listenSocket_));

        //TODO ej - remove when Connect(...) gets really async.
        sleep(1);

        // initiate connections to all hosts with higher id.

        for (ClientId id = my_rank_ + 1; id < addressList.size(); ++id)
        {
            for (uint32_t group = 0; group < kGroupCount; group++) {
                CreateAndConnect(id, addressList[id], group);
            }
        }

        dispatcher.AddRead(listenConnection_, [=](NetConnection& nc) {
                               return ConnectionReceived(nc);
                           });

        while (!InitializationFinished(endpoints.size()))
        {
            LOG << "Client " << my_rank_ << " dispatching.";
            dispatcher.Dispatch();
        }

        for (size_t i = 0; i < connections_.size(); i++) {
            size_t groupId = connections_[i].GetGroupId();
            die_unless(groupId < kGroupCount);
            netGroups_[groupId].AssignConnection(connections_[i]);
        }

        //Could dispose listen connection here.
        listenConnection_.Close();

        LOG << "Client " << my_rank_ << " done";

        for (uint32_t j = 0; j < kGroupCount; j++) {
            // output list of file descriptors connected to partners
            for (size_t i = 0; i != addressList.size(); ++i) {
                if (i == my_rank_) continue;
                LOG << "NetGroup " << j << " link " << my_rank_ << " -> " << i << " = fd "
                    << netGroups_[j].Connection(i).GetSocket().fd();
            }
        }
    }

    void CreateAndConnect(size_t id, lowlevel::SocketAddress& address, size_t group) {
        lowlevel::Socket ns = lowlevel::Socket::Create();
        connections_.emplace_back(ns, group, id);
        connections_.back().SetState(ConnectionState::Disconnected);

        Connect(connections_.back(), address);
    }

    void Connect(NetConnection& connection, lowlevel::SocketAddress& address) {
        die_unless(connection.GetSocket().IsValid());
        die_unless(connection.GetState() == ConnectionState::Disconnected);

        //TOOD ej - Make this really async.
        int res = connection.GetSocket().connect(address);

        connection.SetState(ConnectionState::Connecting);

        if (res == 0) {
            LOG << "Early connect  success. This should not happen.";
            // connect() already successful? this should not be.
            Connected(connection, address);
        }
        else if (errno == EINPROGRESS) {
            // connect is in progress, will wait for completion.
            dispatcher.AddRead(connection, [=](NetConnection& nc) {
                                   return Connected(nc, address);
                               });
        }
        else {
            //Failed to even try the connection - this might be a permanent error.
            connection.SetState(ConnectionState::Invalid);

            throw Exception("Could not connect to client "
                            + std::to_string(connection.GetPeerId()) + " via "
                            + address.ToStringHostPort(), errno);
        }
    }

    void HelloSent(NetConnection& conn) {
        if (conn.GetState() == ConnectionState::TransportConnected) {
            conn.SetState(ConnectionState::HelloSent);
        }
        else if (conn.GetState() == ConnectionState::HelloReceived) {
            conn.SetState(ConnectionState::Connected);
        }
        else {
            die("State mismatch");
        }
    }

    /**
     * @brief Called when a connection initiated by us succeeds.
     * @details
     * @return
     */
    bool Connected(NetConnection& conn, lowlevel::SocketAddress address) {
        int err = conn.GetSocket().GetError();

        if (err != 0) {
            conn.SetState(ConnectionState::Disconnected);
            //Try a reconnect
            //TODO(ej): Figure out if we need a timer here.
            Connect(conn, address);
        }

        die_unless(conn.GetSocket().IsValid());

        conn.SetState(ConnectionState::TransportConnected);

        LOG << "OpenConnections() " << my_rank_ << " connected"
            << " fd=" << conn.GetSocket().fd()
            << " to=" << conn.GetSocket().GetPeerAddress();

        // send welcome message
        const WelcomeMsg hello = { c7a_sign, (uint32_t)conn.GetGroupId(), (uint32_t)my_rank_ };

        dispatcher.AsyncWriteCopy(conn, &hello, sizeof(hello), [=](NetConnection& nc) {
                                      return HelloSent(nc);
                                  });

        LOG << "Client " << my_rank_ << " sent active hello to client ?";

        dispatcher.AsyncRead(conn, sizeof(hello),
                             [&](NetConnection& nc, Buffer&& b) {
                                 ReceiveWelcomeMessage(nc, std::move(b));
                             });

        return false;
    }

    /**
     * @brief Receives and handels a hello message.
     * @details
     *
     * @return
     */
    bool ReceiveWelcomeMessage(NetConnection& conn, Buffer&& buffer) {
        die_unless(conn.GetSocket().IsValid());
        die_unequal(buffer.size(), sizeof(WelcomeMsg));
        die_unequal(conn.GetState(), ConnectionState::HelloSent);

        const WelcomeMsg* msg = reinterpret_cast<const WelcomeMsg*>(buffer.data());
        die_unequal(msg->c7a, c7a_sign);
        //We already know those values since we connected actively.
        //So, check for any errors.
        die_unequal(conn.GetPeerId(), msg->id);
        die_unequal(conn.GetGroupId(), msg->groupId);

        conn.SetState(ConnectionState::Connected);

        LOG << "client " << my_rank_ << " got signature "
            << "from client " << msg->id;

        return false;
    }

    /**
     * @brief Receives and handels a hello message.
     * @details
     *
     * @return
     */
    bool ReceiveWelcomeMessageAndReply(NetConnection& conn, Buffer&& buffer) {
        die_unless(conn.GetSocket().IsValid());
        die_unless(conn.GetState() != ConnectionState::TransportConnected);

        const WelcomeMsg* msg = reinterpret_cast<const WelcomeMsg*>(buffer.data());
        die_unequal(msg->c7a, c7a_sign);
        LOG << "client " << my_rank_ << " got signature "
            << "from client " << msg->id;

        conn.SetState(ConnectionState::HelloReceived);

        const WelcomeMsg hello = { c7a_sign, msg->groupId, (ClientId)my_rank_ };

        // send welcome message

        conn.SetPeerId(msg->id);
        conn.SetGroupId(msg->groupId);
        dispatcher.AsyncWriteCopy(conn, &hello, sizeof(hello), [=](NetConnection& nc) {
                                      return HelloSent(nc);
                                  });

        LOG << "Client " << my_rank_ << " sent passive hello to client " << msg->id;

        return false;
    }

    bool ConnectionReceived(NetConnection& conn) {
        connections_.emplace_back(conn.GetSocket().accept());
        die_unless(connections_.back().GetSocket().IsValid());

        conn.SetState(ConnectionState::TransportConnected);

        LOG << "OpenConnections() " << my_rank_ << " accepted connection"
            << " fd=" << connections_.back().GetSocket().fd()
            << " from=" << connections_.back().GetPeerAddress();

        // wait for welcome message from other side
        dispatcher.AsyncRead(connections_.back(), sizeof(WelcomeMsg),
                             [&](NetConnection& nc, Buffer&& b) {
                                 ReceiveWelcomeMessageAndReply(nc, std::move(b));
                             });

        // wait for more connections?
        return true;
    }

    NetGroup & GetSystemNetGroup() {
        return netGroups_[0];
    }

    NetGroup & GetFlowNetGroup() {
        return netGroups_[1];
    }

    NetGroup & GetDataNetGroup() {
        return netGroups_[2];
    }
};

} // namespace net
} // namespace c7a

#endif // !C7A_NET_NET_MANAGER_HEADER

/******************************************************************************/
