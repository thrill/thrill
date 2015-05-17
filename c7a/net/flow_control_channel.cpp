/*******************************************************************************
 * c7a/net/flow_control_channel.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "flow_control_channel.hpp"
#include <cassert>
#include <string>
#include <vector>

namespace c7a {

#ifdef TIMO_DOES_NOT_KNOW_WHAT_TO_KEEP_HEREOF

//################### Base flow control channel.

void FlowControlChannel::SendTo(std::string message, unsigned int destination)
{
    //TODO Emi Error handling
    //Need to notify controller about failure when smth happens here.
    assert(dispatcher->Send(destination, message) == NET_SERVER_SUCCESS);
}

std::string FlowControlChannel::ReceiveFrom(unsigned int source)
{
    std::string message;

    int r = dispatcher->Receive(source, &message);
    assert(r == NET_SERVER_SUCCESS);

    return message;
}

std::string FlowControlChannel::ReceiveFromAny(unsigned int* source)
{
    std::string message;

    int ret = dispatcher->ReceiveFromAny(source, &message);
    die_unless(ret == NET_SERVER_SUCCESS);

    return message;
}

//################### Master flow control channel

std::vector<std::string> MasterFlowControlChannel::ReceiveFromWorkers()
{
    unsigned int id;
    std::vector<std::string> result(dispatcher->endpoints.size());
    for (unsigned int i = 0; i < dispatcher->endpoints.size(); i++)
    {
        if (i == dispatcher->localId) continue;
        std::string r = ReceiveFrom(i);
        //sLOG << "Received from worker id" << id << "r" << r;
        result[i] = r;
    }

    return result;
}

void MasterFlowControlChannel::BroadcastToWorkers(const std::string& value)
{
    for (ExecutionEndpoint endpoint : dispatcher->endpoints) {
        if (endpoint.id != dispatcher->localId) {
            SendTo(value, endpoint.id);
        }
    }
}

std::vector<std::vector<std::string> > MasterFlowControlChannel::AllToAll()
{
    unsigned int count = dispatcher->endpoints.size() - 1;
    std::vector<std::vector<std::string> > results(count + 1);

    unsigned int id;
    for (unsigned int i = 0; i < count * count; i++) {
        results[id].push_back(ReceiveFromAny(&id));
        if (id + 1 == dispatcher->localId) {
            results[id].push_back("");
        }
    }

    return results;
}

//################### Worker flow control channel

void WorkerFlowControlChannel::SendToMaster(std::string value)
{
    SendTo(value, dispatcher->masterId);
}

std::string WorkerFlowControlChannel::ReceiveFromMaster()
{
    return ReceiveFrom(dispatcher->masterId);
}

std::vector<std::string> WorkerFlowControlChannel::AllToAll(std::vector<std::string> messages)
{
    for (ExecutionEndpoint endpoint : dispatcher->endpoints) {
        if (endpoint.id != dispatcher->localId) {
            SendTo(messages[endpoint.id], endpoint.id);
            SendTo(messages[endpoint.id], dispatcher->masterId);
        }
    }

    unsigned int id;
    std::vector<std::string> result(dispatcher->endpoints.size());
    for (unsigned int i = 0; i < dispatcher->endpoints.size() - 1; i++) {
        result[id] = ReceiveFromAny(&id);
    }

    return result;
}

#endif // TIMO_DOES_NOT_KNOW_WHAT_TO_KEEP_HEREOF

} // namespace c7a

/******************************************************************************/
