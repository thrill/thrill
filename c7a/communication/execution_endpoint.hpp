#pragma once

#include <iostream>
#include <sstream>
#include <string>
#include <cstdlib>

namespace c7a {
namespace communication {

class ExecutionEndpoint;
typedef std::vector<ExecutionEndpoint> ExecutionEndpoints;

//represents a piece of execution hardware that has some endpoint
class ExecutionEndpoint {
public:
    //store some kind of endpoint information here
    const unsigned int id;
    const int port;
    const std::string host;

    ExecutionEndpoint(unsigned int id, std::string host, int port)
        : id(id), port(port), host(host) { }

    static ExecutionEndpoints ParseEndpointList(std::string str) {
        std::stringstream stream;
        stream << str;
        ExecutionEndpoints endpoints;

        std::string endpoint;
        int workerId = 0;

        while(stream >> endpoint) {
            endpoints.push_back(ParseEndpoint(endpoint, workerId));

            workerId++;
        }
        return endpoints;
    }
    static ExecutionEndpoint ParseEndpoint(std::string endpoint, int workerId) {
        int seperator = endpoint.find(":");
        std::string host = endpoint.substr(0, seperator);
        int port = strtol(endpoint.substr(seperator + 1).c_str(),0,10);

        return ExecutionEndpoint(workerId, host, port);
    }
};
}}
