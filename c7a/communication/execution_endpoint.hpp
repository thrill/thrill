/*******************************************************************************
 * c7a/communication/execution_endpoint.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#ifndef C7A_COMMUNICATION_EXECUTION_ENDPOINT_HEADER
#define C7A_COMMUNICATION_EXECUTION_ENDPOINT_HEADER

#include <iostream>
#include <sstream>
#include <string>
#include <cstdlib>

namespace c7a {

class ExecutionEndpoint;
typedef std::vector<ExecutionEndpoint> ExecutionEndpoints;

/*!
 * Identifies a worker, contains its host and port.
 */
class ExecutionEndpoint
{
public:
    //store some kind of endpoint information here
    const unsigned int id;
    const int port;
    const std::string host;

    ExecutionEndpoint(unsigned int id, std::string host, int port)
        : id(id), port(port), host(host) { }

    static ExecutionEndpoints ParseEndpointList(std::string str)
    {
        std::stringstream stream;
        stream << str;
        ExecutionEndpoints endpoints;

        std::string endpoint;
        int workerId = 0;

        while (stream >> endpoint) {
            endpoints.push_back(ParseEndpoint(endpoint, workerId));

            workerId++;
        }
        return endpoints;
    }
    static ExecutionEndpoint ParseEndpoint(std::string endpoint, int workerId)
    {
        int seperator = endpoint.find(":");
        std::string host = endpoint.substr(0, seperator);
        int port = strtol(endpoint.substr(seperator + 1).c_str(), 0, 10);

        return ExecutionEndpoint(workerId, host, port);
    }
};

} // namespace c7a

#endif // !C7A_COMMUNICATION_EXECUTION_ENDPOINT_HEADER

/******************************************************************************/
