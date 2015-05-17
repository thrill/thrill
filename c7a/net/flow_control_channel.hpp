/*******************************************************************************
 * c7a/net/flow_control_channel.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_FLOW_CONTROL_CHANNEL_HEADER
#define C7A_NET_FLOW_CONTROL_CHANNEL_HEADER

#include <string>
#include <vector>
#include "net_group.hpp"

namespace c7a {

#ifdef TIMO_DOES_NOT_KNOW_WHAT_TO_KEEP_HEREOF

/**
 * @brief Provides a blocking collection for communication.
 * @details This should be used for flow control.
 *
 */
class FlowControlChannel
{
protected:
    NetDispatcher* dispatcher;

public:
    explicit FlowControlChannel(NetDispatcher* dispatcher) : dispatcher(dispatcher) { }
    void SendTo(std::string message, unsigned int destination);     //TODO call-by-value is only tmp here and two lines below
    std::string ReceiveFrom(unsigned int source);
    std::string ReceiveFromAny(unsigned int* source = NULL);
};

/**
 * @brief Provides a blocking collection for communication.
 * @details This should be used for flow control by the master.
 * Each method call has to correspond to a similar method call
 * on the worker.
 *
 */
class MasterFlowControlChannel : public FlowControlChannel
{
public:
    explicit MasterFlowControlChannel(NetDispatcher* dispatcher)
        : FlowControlChannel(dispatcher) { }

    /**
     * @brief Receives a value from each worker in the system.
     * @details This method is blocking.
     *
     * @return The received values.
     */
    std::vector<std::string> ReceiveFromWorkers();

    /**
     * @brief Broadcasts a single value to all workers.
     * @details This method is blocking.
     *
     * @param value The value to send.
     */
    void BroadcastToWorkers(const std::string& value);

    /**
     * @brief Receives all values that were transmitted from all workers
     * to all other workers.
     * @details This method is blocking. The message sent from the i-th worker and received by the j-th
     * worker is stored at the index [i-1][j-1].
     * @return The received data.
     */
    std::vector<std::vector<std::string> > AllToAll();
};

/**
 * @brief Provides a blocking collection for communication.
 * @details This should be used for flow control by the worker.
 * Each method call has to correspond to a similar method call
 * on the master.
 *
 */
class WorkerFlowControlChannel : public FlowControlChannel
{
public:
    explicit WorkerFlowControlChannel(NetDispatcher* dispatcher)
        : FlowControlChannel(dispatcher) { }

    /**
     * @brief Sends a single value to the master.
     * @details This method is blocking.
     *
     * @param value The value to send to the master.
     */
    void SendToMaster(std::string message);     //TODO use ref again

    /**
     * @brief Receives a single value from the master.
     * @details This method is blocking.
     *
     * @return The received value.
     */
    std::string ReceiveFromMaster();

    /**
     * @brief Sends and receives each other worker a message.
     * @details This method is blocking.
     *
     * @param messages The messages to send.
     * The message at index i-1 is sent to worker i.
     * The message from worker j is placed into index j-1.
     * @return The received messages.
     */
    std::vector<std::string> AllToAll(std::vector<std::string> messages);
};

#endif // TIMO_DOES_NOT_KNOW_WHAT_TO_KEEP_HEREOF

} // namespace c7a

#endif // !C7A_NET_FLOW_CONTROL_CHANNEL_HEADER

/******************************************************************************/
