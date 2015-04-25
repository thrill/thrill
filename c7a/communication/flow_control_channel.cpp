/*******************************************************************************
 * c7a/communication/flow_control_channel.cpp
 *
 ******************************************************************************/

#include "flow_control_channel.hpp"

namespace c7a {
namespace communication {

//################### Base flow control channel. 

void FlowControlChannel::sendTo(const std::string &message, int destination) 
{
	//TODO Emi Error handling 
	//Need to notify controller about failure when smth happens here. 
	dispatcher->Send(dest, (void*)message.str(), message.length());
}

const std::string &FlowControlChannel::receiveFrom(int source) 
{
	void* buf;
	size_t len;
	dispatcher->Receive(source, &buf, &len);

	return std::string(source, (char*) buf, len);
}

//################### Master flow control channel 

std::vector<std::string> MasterFlowControlChannel::receiveFromWorkers()
{
	return std::vector<std::string>;
}

void MasterFlowControlChannel::broadcastToWorkers(const std::string &value)
{
	for(ExecutionEndpoint endpoint : dispatcher->endpoints) {
		if(endpoint.id != dispatcher->localId) {
			sendTo(endpoint.id, value);
		}
	}
}

std::vector<std::vector<std::string> > MasterFlowControlChannel::allToAll()
{

}

//################### Worker flow control channel 

void WorkerFlowControlChannel::sendToMaster(const std::string &value)
{
	sendTo(dispatcher->msaterId, value);
}

std::string WorkerFlowControlChannel::receiveFromMaster()
{
	return receiveFrom(dispatcher->msaterId);
}

const vector<std::string> &WorkerFlowControlChannel::allToAll(const messages &vector<std::string>)
{
	for(ExecutionEndpoint endpoint : dispatcher->endpoints) {
		if(endpoint.id != dispatcher->localId) {
			sendTo(endpoint.id, messages[endpoint.id]);
		}
	}

	vector<std::string> result;
	for(int i = 0; i < dispatcher->endpoints.count(); i++) {
		//TODO Emi include recv from any from dispatcher. 
	}

	return result;
}

}}
