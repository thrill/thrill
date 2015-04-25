/*******************************************************************************
 * c7a/communication/flow_control_channel.cpp
 *
 ******************************************************************************/

#include "flow_control_channel.hpp"

namespace c7a {
namespace communication {

void FlowControlChannel::sendTo(const std::string &message, int destination) 
{
	//TODO Emi Error handling. 
	dispatcher.Send(dest, (void*)message.str(), message.length());
}

const std::string &FlowControlChannel::receiveFrom(int source) 
{
	void* buf;
	size_t len;
	dispatcher.Receive(source, &buf, &len);

	return std::string(source, (char*) buf, len);
}

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
