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

const std::string &FlowControlChannel::receiveFromAny(int *source) 
{
	void* buf;
	int dummy;
	size_t len;

	if(source == NULL)
		source = &dummy;

	dispatcher->ReceiveFromAny(source, &buf, &len);

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
	int count = dispatcher->endpoints.count() - 1;
	vector<std::vector<std::string> > results(count);

	int id;
	for(int i = 0; i < count * count; i++) {
		results[id].push_back(receiveFromAny(&id));
		if(id + 1== localId) {
			results[id].push_back("");
		}
	}

	return results;
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
			sendTo(dispatcher->Master, messages[endpoint.id]);
		}
	}

	int id;
	vector<std::string> result(dispatcher->endpoints.coint() - 1);
	for(int i = 0; i < dispatcher->endpoints.count() - 1; i++) {
		result[id] = receiveFromAny(&id)
	}

	return result;
}

}}
