/*******************************************************************************
 * c7a/communication/communication_manager.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include "communication_manager.hpp"

namespace c7a {

void CommunicationManager::Initialize(std::vector<ExecutionEndpoint> endpoints)
{
    (void)endpoints;
}

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
