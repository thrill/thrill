/*******************************************************************************
 * c7a/communication/flow_control_channel.cpp
 *
 ******************************************************************************/

#include "flow_control_channel.hpp"

namespace c7a {
namespace communication {

//################### Base flow control channel. 

void FlowControlChannel::sendTo(std::string message, int destination) 
{
	//TODO Emi Error handling 
	//Need to notify controller about failure when smth happens here. 
	dispatcher->Send(destination, (void*)message.c_str(), message.length());
}

std::string FlowControlChannel::receiveFrom(int source) 
{
	void* buf;
	size_t len;
	dispatcher->Receive(source, &buf, &len);

    return "";
//TODO	return std::string(source, (char*) buf, len);
}

std::string FlowControlChannel::receiveFromAny(int *source) 
{
	void* buf;
	int dummy;
	size_t len;

	if(source == NULL)
		source = &dummy;

	dispatcher->ReceiveFromAny(source, &buf, &len);

    return "";
	//TODO return std::string(source, (char*) buf, len);
}

//################### Master flow control channel 

std::vector<std::string> MasterFlowControlChannel::receiveFromWorkers()
{
	return {};
}

void MasterFlowControlChannel::broadcastToWorkers(const std::string &value)
{
	for(ExecutionEndpoint endpoint : dispatcher->endpoints) {
		if(endpoint.id != dispatcher->localId) {
			sendTo(value, endpoint.id);
		}
	}
}

std::vector<std::vector<std::string> > MasterFlowControlChannel::allToAll()
{
	int count = dispatcher->endpoints.size() - 1;
    std::vector<std::vector<std::string> > results(count);

	int id;
	for(int i = 0; i < count * count; i++) {
		results[id].push_back(receiveFromAny(&id));
		if(id + 1== dispatcher->localId) {
			results[id].push_back("");
		}
	}

	return results;
}

//################### Worker flow control channel 

void WorkerFlowControlChannel::sendToMaster(std::string value)
{
	sendTo(value, dispatcher->masterId);
}

std::string WorkerFlowControlChannel::receiveFromMaster()
{
	return receiveFrom(dispatcher->masterId);
}

std::vector<std::string> WorkerFlowControlChannel::allToAll(std::vector<std::string> messages)
{
	for(ExecutionEndpoint endpoint : dispatcher->endpoints) {
		if(endpoint.id != dispatcher->localId) {
			sendTo(messages[endpoint.id], endpoint.id);
			sendTo(messages[endpoint.id], dispatcher->masterId);
		}
	}

	int id;
    std::vector<std::string> result(dispatcher->endpoints.size() - 1);
	for(int i = 0; i < dispatcher->endpoints.size() - 1; i++) {
		result[id] = receiveFromAny(&id);
	}

	return result;
}

}}
