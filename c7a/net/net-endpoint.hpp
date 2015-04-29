/*******************************************************************************
 * c7a/net/net-endpoint.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_NET_NET_ENDPOINT_HEADER
#define C7A_NET_NET_ENDPOINT_HEADER

#include <iostream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <vector>

namespace c7a {

/*!
 * Identifies a remote worker in a NetGroup, currently contains only its host
 * address. In future, the master/worker coordination classes should use this to
 * build a NetGroup, or rebuild it after a network failure.
 */
class NetEndpoint
{
public:
    //store some kind of endpoint information here
    const std::string hostport;

    explicit NetEndpoint(const std::string& hostport)
        : hostport(hostport) { }

    static std::vector<NetEndpoint> ParseEndpointList(std::string str)
    {
        std::stringstream stream;
        stream << str;
        std::vector<NetEndpoint> endpoints;

        std::string hostport;

        while (stream >> hostport) {
            endpoints.push_back(NetEndpoint(hostport));
        }
        return endpoints;
    }
};

} // namespace c7a

#endif // !C7A_NET_NET_ENDPOINT_HEADER

/******************************************************************************/
