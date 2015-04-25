/*******************************************************************************
 * c7a/communication/flow_control_channel.cpp
 *
 ******************************************************************************/

#include "flow_control_channel.hpp"

namespace c7a {
namespace communication {

std::vector<std::string> MasterFlowControlChannel::receiveFromWorkers()
{
	return std::vector<std::string>;
}

void MasterFlowControlChannel::broadcastToWorkers(std::string value)
{

}

std::vector<std::vector<std::string> > MasterFlowControlChannel::allToAll()
{

}

void WorkerFlowControlChannel::sendToMaster(std::string value)
{

}

std::string WorkerFlowControlChannel::receiveFromMaster()
{

}

vector<std::string> WorkerFlowControlChannel::allToAll(messages vector<std::string>)
{

}

}}
