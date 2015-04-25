/*******************************************************************************
 * c7a/communication/communication_manager.hpp
 *
 ******************************************************************************/

#ifndef C7A_COMMUNICATION_COMMUNICATION_MANAGER_HEADER
#define C7A_COMMUNICATION_COMMUNICATION_MANAGER_HEADER

#include <vector>
#include "execution_endpoint.hpp"
#include "system_control_channel.hpp"
#include "flow_control_channel.hpp"

namespace c7a {
namespace communication {

/**
 * @brief Manages communication.
 * @details Manages communication and handles errors. 
 */
class CommunicationManager
{
public:
    void Initialize(std::vector<ExecutionEndpoint> endpoints);

    SystemControlChannel *GetSystemControlChannel();

    FlowControlChannel *GetFlowControlChannel();
};

}}

#endif // !C7A_COMMUNICATION_COMMUNICATION_MANAGER_HEADER

