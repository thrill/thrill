/*******************************************************************************
 * c7a/net/endpoint.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_ENDPOINT_HEADER
#define C7A_NET_ENDPOINT_HEADER

#include <iostream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <vector>

namespace c7a {
namespace net {

//! \addtogroup net Network Communication
//! \{

/*!
 * Identifies a remote worker in a NetGroup, currently contains only its host
 * address. In future, the master/worker coordination classes should use this to
 * build a NetGroup, or rebuild it after a network failure.
 */
class Endpoint
{
public:
    //store some kind of endpoint information here
    const std::string hostport;

    //! Creates Endpoint instance from host:port string
    explicit Endpoint(const std::string& hostport)
        : hostport(hostport) { }

    //! Converts strings with space-separated host:ports
    //! to vector of Endpoint instances
    static std::vector<Endpoint> ParseEndpointList(std::string str) {
        std::stringstream stream;
        stream << str;
        std::vector<Endpoint> endpoints;

        std::string hostport;

        while (stream >> hostport) {
            endpoints.push_back(Endpoint(hostport));
        }
        return endpoints;
    }

    //! Converts vector of strings to vector of Endpoint instances
    static std::vector<Endpoint> ParseEndpointList(std::vector<std::string> str) {
        std::vector<Endpoint> endpoints;
        for (const auto& s : str)
            endpoints.push_back(Endpoint(s));
        return endpoints;
    }
};

static inline
std::ostream& operator << (std::ostream& os, const Endpoint& endpoint) {
    return os << endpoint.hostport;
}

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_ENDPOINT_HEADER

/******************************************************************************/
