/*******************************************************************************
 * c7a/net/execution_endpoint.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_NET_EXECUTION_ENDPOINT_HEADER
#define C7A_NET_EXECUTION_ENDPOINT_HEADER

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
    const std::string hostport;

    ExecutionEndpoint(unsigned int id, const std::string& hostport)
        : id(id), hostport(hostport) { }

    static ExecutionEndpoints ParseEndpointList(std::string str)
    {
        std::stringstream stream;
        stream << str;
        ExecutionEndpoints endpoints;

        std::string hostport;
        int workerId = 0;

        while (stream >> hostport) {
            endpoints.push_back(ExecutionEndpoint(workerId, hostport));
            workerId++;
        }
        return endpoints;
    }
};

} // namespace c7a

#endif // !C7A_NET_EXECUTION_ENDPOINT_HEADER

/******************************************************************************/
