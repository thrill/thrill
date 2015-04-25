/*******************************************************************************
 * c7a/communication/flow_control_channel.cpp
 *
 ******************************************************************************/

#include "flow_control_channel.hpp"
#include <cassert>

namespace c7a {
namespace communication {

//################### Base flow control channel. 

void FlowControlChannel::sendTo(std::string message, unsigned int destination) 
{
	//TODO Emi Error handling 
	//Need to notify controller about failure when smth happens here. 
	assert(dispatcher->Send(destination, (void*)message.c_str(), message.length()) == NET_SERVER_SUCCESS);
}

std::string FlowControlChannel::receiveFrom(unsigned int source) 
{
	void* buf;
	size_t len;
	assert(dispatcher->Receive(source, &buf, &len) == NET_SERVER_SUCCESS);

    return std::string((char*) buf, len);
}

std::string FlowControlChannel::receiveFromAny(unsigned int *source) 
{
	void* buf;
	unsigned int dummy;
	size_t len;

	if(source == NULL)
		source = &dummy;

	dispatcher->ReceiveFromAny(source, &buf, &len);

    return std::string((char*) buf, len);
}

//################### Master flow control channel 

std::vector<std::string> MasterFlowControlChannel::receiveFromWorkers()
{	
	unsigned int id;
    std::vector<std::string> result(dispatcher->endpoints.size());
	for(unsigned int i = 0; i < dispatcher->endpoints.size() - 1; i++) {
		result[id] = receiveFromAny(&id);
	}

	return result;
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
	unsigned int count = dispatcher->endpoints.size() - 1;
    std::vector<std::vector<std::string> > results(count  + 1);

	unsigned int id;
	for(unsigned int i = 0; i < count * count; i++) {
		results[id].push_back(receiveFromAny(&id));
		if(id + 1 == dispatcher->localId) {
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

	unsigned int id;
    std::vector<std::string> result(dispatcher->endpoints.size());
	for(unsigned int i = 0; i < dispatcher->endpoints.size() - 1; i++) {
		result[id] = receiveFromAny(&id);
	}

	return result;
}

}}
