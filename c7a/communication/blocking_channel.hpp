/*******************************************************************************
 * c7a/communication/blocking_channel.hpp
 *
 ******************************************************************************/
#pragma once
#include <string>

namespace c7a {
namespace communication {

/**
 * @brief Provides a blocking collection for communication.
 * @details This is the base communication channel. 
 * 
 */
class BlockingChannel
{
private:
	Endpoint endpoint;
public:
	/**
	 * @brief Connects this blocking channel with the given endpoint.
	 * @details This method is blocking. 
	 */
	void connect(Endpoint endpoint);
	/**
	 * @brief Disconnects this channel from its counterpart.
	 */
	void disconnect();
	/**
	 * @brief Sends a message. 
	 * @details This method blocks.
	 * 
	 * @param message The message to send. 
	 */
	void send(const std::string &message);

	/**
	 * @brief Receives a message.
	 * @details This method blocks.
	 * @return The received message. 
	 */	
	const std::string &recv();
};

}}