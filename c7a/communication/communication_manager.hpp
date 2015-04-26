/*******************************************************************************
 * c7a/communication/communication_manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#ifndef C7A_COMMUNICATION_COMMUNICATION_MANAGER_HEADER
#define C7A_COMMUNICATION_COMMUNICATION_MANAGER_HEADER

#include <vector>
#include "execution_endpoint.hpp"
#include "system_control_channel.hpp"
#include "flow_control_channel.hpp"

namespace c7a {

/**
 * @brief Manages communication.
 * @details Manages communication and handles errors.
 */
class CommunicationManager
{
public:
    void Initialize(std::vector<ExecutionEndpoint> endpoints);

    SystemControlChannel * GetSystemControlChannel();

    FlowControlChannel * GetFlowControlChannel();
};

} // namespace c7a

#endif // !C7A_COMMUNICATION_COMMUNICATION_MANAGER_HEADER

/******************************************************************************/
