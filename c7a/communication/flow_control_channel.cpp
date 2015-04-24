/*******************************************************************************
 * c7a/communication/flow_control_channel.cpp
 *
 ******************************************************************************/
#include "communication_manager.hpp"

namespace c7a {
namespace communication {

template <typename T>
std::vector<T> MasterFlowControlChannel::receiveFromWorkers()
{
	return std::vector<T>;
}

template <typename T>
void MasterFlowControlChannel::broadcastToWorkers(T value)
{

}

template <typename T>
std::vector<T> MasterFlowControlChannel::prefixSum()
{

}

template <typename T>
std::vector<std::vector<T> > MasterFlowControlChannel::allToAll()
{

}

template <typename T>
void WorkerFlowControlChannel::sendToMaster(T value)
{

}

template <typename T>
T WorkerFlowControlChannel::receiveFromMaster()
{

}

template <typename T>
T WorkerFlowControlChannel::prefixSum(T value)
{

}

template <typename T>
vector<T> WorkerFlowControlChannel::allToAll(messages vector<T>)
{

}

}}
