/*******************************************************************************
 * c7a/net/communication_manager.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "communication_manager.hpp"

namespace c7a {

void CommunicationManager::Initialize(const std::vector<NetEndpoint>& /* endpoints */)
{ }

SystemControlChannel* CommunicationManager::GetSystemControlChannel()
{
    return NULL;
}

// FlowControlChannel* CommunicationManager::GetFlowControlChannel()
// {
//     return NULL;
// }

} // namespace c7a

/******************************************************************************/
