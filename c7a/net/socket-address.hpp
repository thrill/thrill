/*******************************************************************************
 * c7a/net/socket-address.hpp
 *
 * Implements lookups and conversions to low-level socket address structs.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_NET_SOCKET_ADDRESS_HEADER
#define C7A_NET_SOCKET_ADDRESS_HEADER

#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <iosfwd>

#include <netinet/in.h>

namespace c7a {

/*!
 * SocketAddress is an abstract class used to unify the two different IPv4 and
 * IPv6 socket address representation. It is derived into IPv4Address and
 * IPv6Address only for direct initialization, in general one just uses
 * SocketAddress("localhost:1234") with appropriate resolution into a sockaddr.
 */
class SocketAddress
{
    static const bool debug = true;

protected:
    //! Enclosed IPv4, IPv6 or other socket address structure. Used as a struct
    //! sockaddr_in or a struct sockaddr_in6. sizeof(sockaddr_in6) is
    //! (currently) 28.
    union m_addr_union
    {
        struct sockaddr     generic;
        struct sockaddr_in  in;
        struct sockaddr_in6 in6;
    } m_addr;

    //! Return value of the last getaddrinfo() call. Used to output nice user
    //! messages.
    int m_resolve_error;

public:
    //! Create empty invalid address object by clearing all bytes.
    SocketAddress()
        : m_resolve_error(0)
    {
        memset(&m_addr, 0, sizeof(m_addr));
    }

    //! Create a socket address object with the given sockaddr data.
    SocketAddress(struct sockaddr* sa, socklen_t salen);

    //! Create a socket address object and resolve the given host:port using
    //! getaddrinfo(). Check result with is_valid().
    SocketAddress(const std::string& hostport);

    //! Create a socket address object and resolve the given host name using
    //! getaddrinfo(). Check result with is_valid().
    SocketAddress(const char* hostname, const char* servicename);

    //! Return pointer to enclosed address as a generic sockattr struct.
    const struct sockaddr * sockaddr() const
    {
        return &m_addr.generic;
    }

    //! Return total length of enclosed sockaddr structure.
    socklen_t get_socklen() const
    {
        return sizeof(m_addr);
    }

    //! Returns true if the enclosed socket address is a valid IPv4 or IPv6
    //! address.
    bool is_valid() const
    {
        return (sockaddr()->sa_family == AF_INET) ||
               (sockaddr()->sa_family == AF_INET6);
    }

    //! Returns true if the enclosed socket address is a IPv4 address.
    bool is_ipv4() const
    {
        return (sockaddr()->sa_family == AF_INET);
    }

    //! Returns true if the enclosed socket address is a IPv6 address.
    bool is_ipv6() const
    {
        return (sockaddr()->sa_family == AF_INET6);
    }

    //! Cast the enclosed sockaddr into the sockaddr_in IPv4 structure.
    struct sockaddr_in * sockaddr_in()
    {
        return &m_addr.in;
    }

    //! Cast the enclosed sockaddr into the sockaddr_in IPv4 structure. Const
    //! version.
    const struct sockaddr_in * sockaddr_in() const
    {
        return &m_addr.in;
    }

    //! Cast the enclosed sockaddr into the sockaddr_in6 IPv6 structure.
    struct sockaddr_in6 * sockaddr_in6()
    {
        return &m_addr.in6;
    }

    //! Cast the enclosed sockaddr into the sockaddr_in6 IPv6 structure. Const
    //! version.
    const struct sockaddr_in6 * sockaddr_in6() const
    {
        return &m_addr.in6;
    }

    //! Return the enclosed socket address as a string.
    std::string str() const;

    //! Return the currently set port address in host byte-order.
    uint16_t get_port() const
    {
        if (sockaddr()->sa_family == AF_INET)
        {
            return ntohs(sockaddr_in()->sin_port);
        }
        else if (sockaddr()->sa_family == AF_INET6)
        {
            return ntohs(sockaddr_in6()->sin6_port);
        }
        else
            return 0;
    }

    //! Change the currently set port address.
    void set_port(uint16_t port)
    {
        if (sockaddr()->sa_family == AF_INET)
        {
            sockaddr_in()->sin_port = htons(port);
        }
        else if (sockaddr()->sa_family == AF_INET6)
        {
            sockaddr_in6()->sin6_port = htons(port);
        }
    }

    //! Resolve the given host name using getaddrinfo() and replace this object
    //! with the first socket address if found.
    bool resolve(const char* hostname,
                 const char* servicename = NULL);

    //! Resolve the given host name using getaddrinfo() and return only the
    //! first socket address if found.
    static SocketAddress resolve_one(const char* hostname,
                                     const char* servicename = NULL);

    //! Parse the address for a :port notation and then resolve the
    //! given host name using getaddrinfo() and return only the first
    //! socket address if found. Uses defaultservice if no port is
    //! found in the hostname.
    static SocketAddress resolve_withport(const char* hostname,
                                          const char* defaultservice);

    //! Resolve the given host name using getaddrinfo() and return all
    //! resulting socket addresses as a vector.
    static std::vector<SocketAddress>
    resolve_all(const char* hostname, const char* servicename = NULL);

    //! Return textual message of the last error occurring in the resolve
    //! method.
    const char * get_resolve_error() const;

    //! Make the socket address ostream-able.
    friend std::ostream& operator << (std::ostream& os, const SocketAddress& sa);
};

class IPv4Address : public SocketAddress
{
public:
    //! Create an IPv4 address and initialize only the port part.
    explicit IPv4Address(uint16_t port)
        : SocketAddress()
    {
        m_addr.in.sin_family = AF_INET;
        m_addr.in.sin_port = htons(port);
    }

    //! Create an IPv4 address object with initialized address and port parts.
    IPv4Address(uint32_t addr, uint16_t port)
        : SocketAddress()
    {
        m_addr.in.sin_family = AF_INET;
        m_addr.in.sin_addr.s_addr = addr;
        m_addr.in.sin_port = htons(port);
    }

    //! Create an IPv4 address object with initialized address and port parts.
    IPv4Address(struct in_addr& addr, uint16_t port)
        : SocketAddress()
    {
        m_addr.in.sin_family = AF_INET;
        m_addr.in.sin_addr = addr;
        m_addr.in.sin_port = htons(port);
    }

    //! Create an IPv4 address object and copy the given sockaddr_in structure.
    explicit IPv4Address(struct sockaddr_in& sa)
        : SocketAddress()
    {
        m_addr.in = sa;
    }

    //! Create an IPv4 address object and initialize it with the given ip
    //! address string, which is given in "ddd.ddd.ddd.ddd" format. You must
    //! check with is_valid() if the conversion was successfull.
    explicit IPv4Address(const char* ipstring, uint16_t port = 0);
};

class IPv6Address : public SocketAddress
{
public:
    //! Create an IPv6 address and initialize only the port part.
    explicit IPv6Address(uint16_t port)
        : SocketAddress()
    {
        m_addr.in6.sin6_family = AF_INET6;
        m_addr.in6.sin6_port = htons(port);
    }

    //! Create an IPv6 address object with initialized address and port parts.
    IPv6Address(uint8_t addr[16], uint16_t port)
        : SocketAddress()
    {
        m_addr.in6.sin6_family = AF_INET6;
        memcpy(&m_addr.in6.sin6_addr, addr, 16 * sizeof(uint8_t));
        m_addr.in6.sin6_port = htons(port);
    }

    //! Create an IPv4 address object with initialized address and port parts.
    IPv6Address(struct in6_addr& addr, uint16_t port)
        : SocketAddress()
    {
        m_addr.in6.sin6_family = AF_INET6;
        m_addr.in6.sin6_addr = addr;
        m_addr.in6.sin6_port = htons(port);
    }

    //! Create an IPv4 address object and copy the given sockaddr_in structure.
    explicit IPv6Address(struct sockaddr_in6& sa)
        : SocketAddress()
    {
        m_addr.in6 = sa;
    }

    //! Create an IPv6 address object and initialize it with the given ip
    //! address string, which is given in some IPv6 format. You must check with
    //! is_valid() if the conversion was successfull.
    explicit IPv6Address(const char* ipstring, uint16_t port = 0);
};

} // namespace c7a

#endif // !C7A_NET_SOCKET_ADDRESS_HEADER

/******************************************************************************/
