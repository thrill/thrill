/*******************************************************************************
 * c7a/communication/flow_control_channel.hpp
 *
 ******************************************************************************/
#pragma once
#include <string>
#include <vector>
#include "net_dispatcher.hpp"

namespace c7a {
namespace communication {

/**
 * @brief Provides a blocking collection for communication.
 * @details This should be used for flow control. 
 * 
 */
class FlowControlChannel
{
protected:
	NetDispatcher *dispatcher;	
public:
	FlowControlChannel(NetDispatcher *dispatcher) : dispatcher(dispatcher) { }
	void sendTo(std::string message, unsigned int destination); //TODO call-by-value is only tmp here and two lines below
	std::string receiveFrom(unsigned int source);
	std::string receiveFromAny(unsigned int *source = NULL);

};

/**
 * @brief Provides a blocking collection for communication.
 * @details This should be used for flow control by the master.
 * Each method call has to correspond to a similar method call 
 * on the worker. 
 * 
 */
class MasterFlowControlChannel : FlowControlChannel
{
public:
	/**
	 * @brief Receives a value from each worker in the system.
	 * @details This method is blocking. 
	 * 
	 * @return The received values. 
	 */
	std::vector<std::string> receiveFromWorkers();

	/**
	 * @brief Broadcasts a single value to all workers.
	 * @details This method is blocking. 
	 * 
	 * @param value The value to send. 
	 */
	void broadcastToWorkers(const std::string &value);
	
	/**
	 * @brief Receives all values that were transmitted from all workers 
	 * to all other workers. 
	 * @details This method is blocking. The message sent from the i-th worker and received by the j-th 
	 * worker is stored at the index [i-1][j-1]. 
	 * @return The received data. 
	 */
	std::vector<std::vector<std::string> > allToAll();

};


/**
 * @brief Provides a blocking collection for communication.
 * @details This should be used for flow control by the worker.
 * Each method call has to correspond to a similar method call 
 * on the master. 
 * 
 */
class WorkerFlowControlChannel : FlowControlChannel
{
	/**
	 * @brief Sends a single value to the master.
	 * @details This method is blocking.
	 * 
	 * @param value The value to send to the master.
	 */
	void sendToMaster(std::string message); //TODO use ref again

	/**
	 * @brief Receives a single value from the master.
	 * @details This method is blocking. 
	 * 
	 * @return The received value. 
	 */
	std::string receiveFromMaster();

	/**
	 * @brief Sends and receives each other worker a message. 
	 * @details This method is blocking. 
	 * 
	 * @param messages The messages to send. 
	 * The message at index i-1 is sent to worker i.
	 * The message from worker j is placed into index j-1. 
	 * @return The received messages. 
	 */
	std::vector<std::string> allToAll(std::vector<std::string> messages);
};

}}
