/*******************************************************************************
 * c7a/net/communication_manager.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include "communication-manager.hpp"

namespace c7a {

namespace net {

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

} // namespace net

} // namespace c7a

/******************************************************************************/
