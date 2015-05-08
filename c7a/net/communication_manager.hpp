/*******************************************************************************
 * c7a/net/communication_manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_COMMUNICATION_MANAGER_HEADER
#define C7A_NET_COMMUNICATION_MANAGER_HEADER

#include <vector>
#include "net-endpoint.hpp"
#include "system_control_channel.hpp"
#include "flow_control_channel.hpp"

namespace c7a {

/**
 * @brief Manages communication.
 * @details Manages communication and handles errors.
 */
class CommunicationManager
{

private:
	NetGroup* systemNetGroup;
	NetGroup* flowNetGroup;
	NetGroup* dataNetGroup;
public:
	void Initialize(size_t myRank, const std::vector<NetEndpoint>& endpoints)
	{ 
		systemNetGroup = new NetGroup(myRank, endpoints);
		//TODO - Create a socket with OS assigned port number,
		//Send portnumber to all others. 
		//Receive portnumbers from all others.
		//Stick server socket and portnumbers into NetGroup and init. 

		//Do this. Twice. 
		
	}

	NetGroup* GetSystemNetGroup() {

	}

	NetGroup* GetFlowNetGroup() {

	}

	NetGroup* GetDataNetGroup() {

	}

	void Dispose() {

	}
};

} // namespace c7a

#endif // !C7A_NET_COMMUNICATION_MANAGER_HEADER

/******************************************************************************/
