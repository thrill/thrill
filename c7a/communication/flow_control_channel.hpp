/*******************************************************************************
 * c7a/communication/flow_control_channel.hpp
 *
 ******************************************************************************/
#pragma once

#include "flow_control_channel.hpp"

namespace c7a {
namespace communication {

/**
 * @brief Provides a blocking collection for communication.
 * @details This should be used for flow control. 
 * 
 */
class FlowControlChannel
{
};

/**
 * @brief Provides a blocking collection for communication.
 * @details This should be used for flow control by the master.
 * Each method call has to correspond to a similar method call 
 * on the worker. 
 * 
 */
class MasterFlowControlChannel
{
public:
	/**
	 * @brief Receives a value from each worker in the system.
	 * @details This method is blocking. 
	 * 
	 * @tparam T The type of the value to receive. 
	 * @return The received values. 
	 */
	template <typename T>
	std::vector<T> receiveFromWorkers();

	/**
	 * @brief Broadcasts a single value to all workers.
	 * @details This method is blocking. 
	 * 
	 * @param value The value to send. 
	 */
	template <typename T>
	void broadcastToWorkers(T value);
	
	/**
	 * @brief Receives the prefix-sum results as corresponding to each worker. 
	 * @details This method is blocking. The prefix sum of the n-th worker is 
	 * stored at the n-1-th index of the vector.
	 * 
	 * @tparam T The type of the values to receive. 
	 * @return The received values. 
	 */
	template <typename T>
	std::vector<T> prefixSum();

	/**
	 * @brief Receives all values that were transmitted from all workers 
	 * to all other workers. 
	 * @details This method is blocking. The message sent from the i-th worker and received by the j-th 
	 * worker is stored at the index [i-1][j-1].
	 * @tparam T The type of the values to receive. 
	 * @return The received data. 
	 */
	template <typename T>
	std::vector<std::vector<T> > allToAll();

};


/**
 * @brief Provides a blocking collection for communication.
 * @details This should be used for flow control by the worker.
 * Each method call has to correspond to a similar method call 
 * on the master. 
 * 
 */
class WorkerFlowControlChannel
{
	/**
	 * @brief Sends a single value to the master.
	 * @details This method is blocking.
	 * 
	 * @param value The value to send to the master.
	 */
	template <typename T>
	void sendToMaster(T value);

	/**
	 * @brief Receives a single value from the master.
	 * @details This method is blocking. 
	 * 
	 * @tparam T The type of the value to receive. 
	 * @return The received value. 
	 */
	template <typename T>
	T receiveFromMaster();

	/**
	 * @brief Calculates a prefix sum. 
	 * @details This method is blocking. 
	 * 
	 * @param value The type of the value. 
	 * @tparam T The type of the value to send and receive. 
	 * @return The prefix sum of all previous workers, 
	 * including the current worker. 
	 */
	template <typename T>
	T prefixSum(T value);

	/**
	 * @brief Sends and receives each other worker a message. 
	 * @details This method is blocking. 
	 * 
	 * @param messages The messages to send. 
	 * The message at index i-1 is sent to worker i.
	 * The message from worker j is placed into index j-1. 
	 * @tparam T The type of the message to send.
	 * @return The received messages. 
	 */
	template <typename T>
	std::vector<T> allToAll(std::vector<T> messages);
};

}}