/*******************************************************************************
 * c7a/communication/socket_address.cpp
 *
 * Implements lookups and conversions to low-level socket address structs.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/communication/socket_address.hpp>

#include <string>
#include <netdb.h>
#include <arpa/inet.h>
#include <errno.h>

#include <c7a/common/logger.hpp>
#include <c7a/communication/socket.hpp>

namespace c7a {

/******************************************************************************/

SocketAddress::SocketAddress(struct sockaddr* sa, socklen_t salen)
    : m_resolve_error(0)
{
    memcpy(&m_addr, sa, std::min<socklen_t>(salen, sizeof(m_addr)));
}

SocketAddress::SocketAddress(const char* hostport)
{
    std::string host = hostport;
    size_t colonpos = host.rfind(':');
    if (colonpos == std::string::npos)
    {
        resolve(hostport);
    }
    else
    {
        std::string port = host.substr(colonpos + 1);
        host.erase(colonpos);
        resolve(host.c_str(), port.c_str());
    }
}

SocketAddress::SocketAddress(const char* hostname, const char* servicename)
{
    resolve(hostname, servicename);
}

std::string SocketAddress::str() const
{
    char str[64];
    if (sockaddr()->sa_family == AF_INET)
    {
        if (inet_ntop(AF_INET,
                      &sockaddr_in()->sin_addr, str, sizeof(str)) == NULL)
        {
            sLOG << "Error in inet_ntop: " << strerror(errno);
            return "<error>";
        }
        return str;
    }
    else if (sockaddr()->sa_family == AF_INET6)
    {
        if (inet_ntop(AF_INET6,
                      &sockaddr_in6()->sin6_addr, str, sizeof(str)) == NULL)
        {
            sLOG << "Error in inet_ntop: " << strerror(errno);
            return "<error>";
        }
        return str;
    }
    else
        return "<invalid>";
}

bool SocketAddress::resolve(const char* hostname, const char* servicename)
{
    struct addrinfo* result;
    struct addrinfo hints;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;    /* Allow IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = 0;
    hints.ai_protocol = 0;

    int gai = getaddrinfo(hostname, servicename, &hints, &result);
    if (gai != 0)
    {
        memset(&m_addr, 0, sizeof(m_addr));
        m_resolve_error = gai;
        return false;
    }
    else
    {
        *this = SocketAddress(result->ai_addr, result->ai_addrlen);
        freeaddrinfo(result);
        return is_valid();
    }
}

const char* SocketAddress::get_resolve_error() const
{
    return gai_strerror(m_resolve_error);
}

SocketAddress
SocketAddress::resolve_one(const char* hostname, const char* servicename)
{
    struct addrinfo* result;
    struct addrinfo hints;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;    /* Allow IPv4 or IPv6 */
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
SocketAddress::resolve_withport(const char* hostname,
                                const char* defaultservice)
{
    std::string host = hostname;

    std::string::size_type colonpos = host.rfind(':');
    if (colonpos == std::string::npos)
        return resolve_one(hostname, defaultservice);

    std::string servicename(host, colonpos + 1);
    host.erase(colonpos);

    return resolve_one(host.c_str(), servicename.c_str());
}

std::vector<SocketAddress>
SocketAddress::resolve_all(const char* hostname, const char* servicename)
{
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

    for (struct addrinfo* ap = result; ap != NULL; ap = ap->ai_next)
    {
        salist.push_back(SocketAddress(ap->ai_addr, ap->ai_addrlen));
    }

    freeaddrinfo(result);

    return salist;
}

/******************************************************************************/

IPv4Address::IPv4Address(const char* ipstring, uint16_t port)
    : SocketAddress()
{
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
    : SocketAddress()
{
    struct sockaddr_in6* sin6 = sockaddr_in6();
    sin6->sin6_family = AF_INET6;
    if (inet_pton(AF_INET6, ipstring, &sin6->sin6_addr) <= 0) {
        sin6->sin6_family = 0;
        return;
    }
    sin6->sin6_port = htons(port);
}

} // namespace c7a

/******************************************************************************/
