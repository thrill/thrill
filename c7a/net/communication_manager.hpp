/*******************************************************************************
 * c7a/net/communication_manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_COMMUNICATION_MANAGER_HEADER
#define C7A_NET_COMMUNICATION_MANAGER_HEADER

#include <c7a/net/net-endpoint.hpp>
#include <c7a/net/net-connection.hpp>
#include <c7a/net/net-dispatcher.hpp>
#include <c7a/net/net-group.hpp>

#include <vector>
#include <functional>
#include "net-endpoint.hpp"

#define GROUP_COUNT 3

namespace c7a {

/**
 * @brief Manages communication.
 * @details Manages communication and handles errors.
 */
class CommunicationManager
{
    static const bool debug = false;

private:
	NetGroup* netGroups_[GROUP_COUNT]; //Net groups
    Socket listenSocket_;
    size_t my_rank_;
    int got_connections = -1; //-1 mens uninitialized
    unsigned int accepting;
	NetDispatcher dispatcher;
	
	/**
	 * @brief Converts a c7a endpoint list into a list of socket address.
	 * 
	 * @param endpoints The endpoint list to convert.
	 * @return The socket addresses to use internally. 
	 */
	std::vector<SocketAddress> GetAddressList(const std::vector<NetEndpoint>& endpoints) {
	    std::vector<SocketAddress> addressList;

	    for (const NetEndpoint& ne : endpoints)
	    {
	        addressList.push_back(SocketAddress(ne.hostport));
	        if (!addressList.back().IsValid()) {
	            throw NetException(
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

public:
	void Initialize(size_t my_rank_, const std::vector<NetEndpoint>& endpoints)
	{ 
		if(got_connections != -1) {
			throw new NetException("This communication manager has already been initialized.");
		}

		got_connections = 0;
		accepting = my_rank_;

    	die_unless(my_rank_ < endpoints.size());

    	std::vector<SocketAddress> addressList = GetAddressList(endpoints);


 		listenSocket_ = Socket::Create();
    	listenSocket_.SetReuseAddr();

    	const SocketAddress& lsa = addressList[my_rank_];

	    if (listenSocket_.bind(lsa) != 0)
	        throw NetException("Could not bind listen socket to "
	                           + lsa.ToStringHostPort(), errno);

	    if (listenSocket_.listen() != 0)
	        throw NetException("Could not listen on socket "
	                           + lsa.ToStringHostPort(), errno);

	    // TODO(tb): this sleep waits for other clients to open their ports. do this
	    // differently.
	    sleep(1);

	    // initiate connections to all hosts with higher id.

	    for (ClientId id = my_rank_ + 1; id < addressList.size(); ++id)
	    {
		    for(uint32_t group = 0; group < GROUP_COUNT; group++) {

	    		const WelcomeMsg hello = { c7a_sign, group, (ClientId)my_rank_ };
	        
	        	Socket ns = Socket::Create();

		        // initiate non-blocking TCP connection
		        ns.SetNonBlocking(true);

		        if (ns.connect(addressList[id]) == 0) {
	            // connect() already successful? this should not be.
		            ActiveConnected(ns, hello);
		        }
		        else if (errno == EINPROGRESS) {
		            // connect is in progress, will wait for completion.
		            dispatcher.AddWrite(ns, std::bind(&c7a::CommunicationManager::ActiveConnected, this, std::placeholders::_1, hello));
		        }
		        else {
		            throw NetException("Could not connect to client "
		                               + std::to_string(id) + " via "
		                               + addressList[id].ToStringHostPort(), errno);
		        }
		    }
		}

   		dispatcher.AddRead(listenSocket_, std::bind(&c7a::CommunicationManager::PassiveConnected, this, std::placeholders::_1));

	    while (got_connections != (int)((addressList.size() - 1) * GROUP_COUNT))
	    {
	        dispatcher.Dispatch();
	    }
		
	    LOG << "done";

	    for(uint32_t j = 0; j < GROUP_COUNT; j++) {
	    	// output list of file descriptors connected to partners
		    for (size_t i = 0; i != addressList.size(); ++i) {
		    	if(i == my_rank_) continue;
		        LOG << "NetGroup " << j << " link " << my_rank_ << " -> " << i << " = fd "
		            << netGroups_[j]->Connection(i).GetSocket().GetFileDescriptor();
		    }
		}
	}

	/**
	 * @brief Called when a socket connects.
	 * @details 
	 * @return 
	 */
	bool ActiveConnected(Socket& sock, const WelcomeMsg& hello) {
 		int err = sock.GetError();

	    if (err != 0) {
	        throw NetException(
	                  "OpenConnections() could not connect to client "
	                  + std::to_string(hello.id), err);
	    }

	    LOG << "OpenConnections() " << my_rank_ << " connected"
	        << " fd=" << sock.GetFileDescriptor()
	        << " to=" << sock.GetPeerAddress();

	    // send welcome message
	    sock.SetNonBlocking(false);
	    dispatcher.AsyncWrite(sock, &hello, sizeof(hello));
	    LOG << "sent client " << hello.id;

	    // wait for welcome message from other side
	    dispatcher.AsyncRead(sock, sizeof(hello), std::bind(&c7a::CommunicationManager::ReceiveWelcomeMessage, this, std::placeholders::_1, std::placeholders::_2));

	    return false;
	}

	/**
	 * @brief Receives and handels a hello message. 
	 * @details
	 * 
	 * @return 
	 */
	bool ReceiveWelcomeMessage(Socket &sock, const std::string& buffer) {
		LOG0 << "Message on " << sock.GetFileDescriptor();
        die_unequal(buffer.size(), sizeof(WelcomeMsg));

        const WelcomeMsg* msg = reinterpret_cast<const WelcomeMsg*>(buffer.data());
        die_unequal(msg->c7a, c7a_sign);

        LOG0 << "client " << my_rank_ << " got signature "
             << "from client " << msg->id;

        // assign connection.
        die_unless(!netGroups_[msg->groupId]->Connection(msg->id).GetSocket().IsValid());
       	netGroups_[msg->groupId]->SetConnection(msg->id, NetConnection(sock));
        ++got_connections;

		return true;
	}	

	/**
	 * @brief Receives and handels a hello message. 
	 * @details
	 * 
	 * @return 
	 */
	bool ReceiveWelcomeMessageAndReply(Socket &sock, const std::string& buffer) {
	
		ReceiveWelcomeMessage(sock, buffer);
        const WelcomeMsg* msg = reinterpret_cast<const WelcomeMsg*>(buffer.data());

        const WelcomeMsg hello = { c7a_sign, msg->groupId, (ClientId)my_rank_ };

		// send welcome message
        dispatcher.AsyncWrite(sock, &hello, sizeof(hello));
        LOG << "sent client " << hello.id;

        ++got_connections;

		return true;
	}

	bool PassiveConnected(Socket& sock) {
  		die_unless(accepting > 0);

        Socket ns = sock.accept();
        LOG << "OpenConnections() " << my_rank_ << " accepted connection"
            << " fd=" << ns.GetFileDescriptor()
            << " from=" << ns.GetPeerAddress();

        // wait for welcome message from other side
        dispatcher.AsyncRead(ns, sizeof(WelcomeMsg), std::bind(&c7a::CommunicationManager::ReceiveWelcomeMessageAndReply, this, std::placeholders::_1, std::placeholders::_2));

        // wait for more connections?
        return (--accepting > 0);
	}

	NetGroup* GetSystemNetGroup() {
		return netGroups_[0];
	}

	NetGroup* GetFlowNetGroup() {
		return netGroups_[1];
	}

	NetGroup* GetDataNetGroup() {
		return netGroups_[2];
	}

	void Dispose() {
		//TODO MUHA
	}
};

} // namespace c7a

#endif // !C7A_NET_COMMUNICATION_MANAGER_HEADER

/******************************************************************************/
