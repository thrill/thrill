/*******************************************************************************
 * thrill/net/tcp/socket_address.cpp
 *
 * Implements lookups and conversions to low-level socket address structs.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/net/tcp/socket_address.hpp>

#include <arpa/inet.h>
#include <netdb.h>

#include <algorithm>
#include <cerrno>
#include <string>
#include <vector>

namespace thrill {
namespace net {
namespace tcp {

/******************************************************************************/

SocketAddress::SocketAddress(struct sockaddr* sa, socklen_t salen)
    : resolve_error_(0) {
    memcpy(&sockaddr_, sa, std::min<socklen_t>(salen, sizeof(sockaddr_)));
}

SocketAddress::SocketAddress(const std::string& hostport) {
    std::string host = hostport;
    size_t colonpos = host.rfind(':');
    if (colonpos == std::string::npos)
    {
        Resolve(hostport.c_str());
    }
    else
    {
        std::string port = host.substr(colonpos + 1);
        host.erase(colonpos);
        Resolve(host.c_str(), port.c_str());
    }
}

SocketAddress::SocketAddress(const char* hostname, const char* servicename) {
    Resolve(hostname, servicename);
}

std::string SocketAddress::ToStringHost() const {
    char str[64];
    if (sockaddr()->sa_family == AF_INET)
    {
        if (inet_ntop(AF_INET,
                      &sockaddr_in()->sin_addr, str, sizeof(str)) == nullptr)
        {
            sLOG << "Error in inet_ntop: " << strerror(errno);
            return "<error>";
        }
        return str;
    }
    else if (sockaddr()->sa_family == AF_INET6)
    {
        if (inet_ntop(AF_INET6,
                      &sockaddr_in6()->sin6_addr, str, sizeof(str)) == nullptr)
        {
            sLOG << "Error in inet_ntop: " << strerror(errno);
            return "<error>";
        }
        return str;
    }
    else
        return "<invalid>";
}

std::string SocketAddress::ToStringHostPort() const {
    return ToStringHost() + ":" + std::to_string(GetPort());
}

std::ostream& operator << (std::ostream& os, const SocketAddress& sa) {
    return os << sa.ToStringHostPort();
}

bool SocketAddress::Resolve(const char* hostname, const char* servicename) {
    struct addrinfo* result;
    struct addrinfo hints;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;    /* Allow IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = 0;
    hints.ai_protocol = 0;

    int gai = getaddrinfo(hostname, servicename, &hints, &result);
    if (gai != 0)
    {
        memset(&sockaddr_, 0, sizeof(sockaddr_));
        resolve_error_ = gai;
        return false;
    }
    else
    {
        *this = SocketAddress(result->ai_addr, result->ai_addrlen);
        freeaddrinfo(result);
        return IsValid();
    }
}

const char* SocketAddress::GetResolveError() const {
    return gai_strerror(resolve_error_);
}

SocketAddress
SocketAddress::ResolveOne(const char* hostname, const char* servicename) {
    struct addrinfo* result;
    struct addrinfo hints;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;    /* Allow IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = 0;
    hints.ai_protocol = 0;

    int s = getaddrinfo(hostname, servicename, &hints, &result);
    if (s != 0) {
        return SocketAddress();
    }

    SocketAddress sa(result->ai_addr, result->ai_addrlen);

    freeaddrinfo(result);

    return sa;
}

SocketAddress
SocketAddress::ResolveWithPort(const char* hostname,
                               const char* defaultservice) {
    std::string host = hostname;

    std::string::size_type colonpos = host.rfind(':');
    if (colonpos == std::string::npos)
        return ResolveOne(hostname, defaultservice);

    std::string servicename(host, colonpos + 1);
    host.erase(colonpos);

    return ResolveOne(host.c_str(), servicename.c_str());
}

std::vector<SocketAddress>
SocketAddress::ResolveAll(const char* hostname, const char* servicename) {
    std::vector<SocketAddress> salist;

    struct addrinfo* result;
    struct addrinfo hints;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;    /* Allow IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = 0;
    hints.ai_protocol = 0;

    int s = getaddrinfo(hostname, servicename, &hints, &result);
    if (s != 0) {
        return salist;
    }

    for (struct addrinfo* ap = result; ap != nullptr; ap = ap->ai_next)
    {
        salist.push_back(SocketAddress(ap->ai_addr, ap->ai_addrlen));
    }

    freeaddrinfo(result);

    return salist;
}

/******************************************************************************/

IPv4Address::IPv4Address(const char* ipstring, uint16_t port)
    : SocketAddress() {
    struct sockaddr_in* sin = sockaddr_in();
    sin->sin_family = AF_INET;
    if (inet_pton(AF_INET, ipstring, &sin->sin_addr) <= 0) {
        sin->sin_family = 0;
        return;
    }
    sin->sin_port = htons(port);
}

/******************************************************************************/

IPv6Address::IPv6Address(const char* ipstring, uint16_t port)
    : SocketAddress() {
    struct sockaddr_in6* sin6 = sockaddr_in6();
    sin6->sin6_family = AF_INET6;
    if (inet_pton(AF_INET6, ipstring, &sin6->sin6_addr) <= 0) {
        sin6->sin6_family = 0;
        return;
    }
    sin6->sin6_port = htons(port);
}

} // namespace tcp
} // namespace net
} // namespace thrill

/******************************************************************************/
