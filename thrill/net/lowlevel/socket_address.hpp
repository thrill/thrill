/*******************************************************************************
 * c7a/net/lowlevel/socket_address.hpp
 *
 * Implements lookups and conversions to low-level socket address structs.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_LOWLEVEL_SOCKET_ADDRESS_HEADER
#define C7A_NET_LOWLEVEL_SOCKET_ADDRESS_HEADER

#include <netinet/in.h>
#include <sys/socket.h>

#include <cstdint>
#include <cstring>
#include <iosfwd>
#include <string>
#include <vector>

namespace c7a {
namespace net {
namespace lowlevel {

//! \addtogroup netsock Low Level Socket API
//! \{

/*!
 * SocketAddress is a super class used to unify the two different IPv4 and IPv6
 * socket address representation. It is derived into IPv4Address and IPv6Address
 * only for direct initialization, in general one just uses
 * SocketAddress("localhost:1234") for appropriate resolution into a
 * sockaddr. The SocketAddress object can then be given to various Connect(),
 * Bind() and similar functions of Socket.
 */
class SocketAddress
{
    static const bool debug = false;

protected:
    //! Enclosed IPv4, IPv6 or other socket address structure. Used as a struct
    //! sockaddr_in or a struct sockaddr_in6. sizeof(sockaddr_in6) is
    //! (currently) 28.
    union SockAddrUnion
    {
        struct sockaddr     generic;
        struct sockaddr_in  in;
        struct sockaddr_in6 in6;
    } sockaddr_;

    //! Return value of the last getaddrinfo() call. Used to output nice user
    //! messages.
    int resolve_error_;

public:
    //! Create empty invalid address object by clearing all bytes.
    SocketAddress()
        : resolve_error_(0) {
        memset(&sockaddr_, 0, sizeof(sockaddr_));
    }

    //! Create a socket address object with the given sockaddr data.
    SocketAddress(struct sockaddr* sa, socklen_t salen);

    //! Create a socket address object and resolve the given host:port using
    //! getaddrinfo(). Check result with IsValid().
    explicit SocketAddress(const std::string& hostport);

    //! Create a socket address object and resolve the given host name using
    //! getaddrinfo(). Check result with IsValid().
    SocketAddress(const char* hostname, const char* servicename);

    //! Return pointer to enclosed address as a generic sockattr struct.
    struct sockaddr * sockaddr() {
        return &sockaddr_.generic;
    }

    //! Return pointer to enclosed address as a generic sockattr struct.
    const struct sockaddr * sockaddr() const {
        return &sockaddr_.generic;
    }

    //! Return total length of enclosed sockaddr structure.
    socklen_t socklen() const {
        return sockaddr()->sa_family == AF_INET ? sizeof(sockaddr_.in) :
               sockaddr()->sa_family == AF_INET6 ? sizeof(sockaddr_.in6) : 0;
    }

    //! Returns true if the enclosed socket address is a valid IPv4 or IPv6
    //! address.
    bool IsValid() const {
        return (sockaddr()->sa_family == AF_INET) ||
               (sockaddr()->sa_family == AF_INET6);
    }

    //! Returns true if the enclosed socket address is a IPv4 address.
    bool IsIPv4() const {
        return (sockaddr()->sa_family == AF_INET);
    }

    //! Returns true if the enclosed socket address is a IPv6 address.
    bool IsIPv6() const {
        return (sockaddr()->sa_family == AF_INET6);
    }

    //! Cast the enclosed sockaddr into the sockaddr_in IPv4 structure.
    struct sockaddr_in * sockaddr_in() {
        return &sockaddr_.in;
    }

    //! Cast the enclosed sockaddr into the sockaddr_in IPv4 structure. Const
    //! version.
    const struct sockaddr_in * sockaddr_in() const {
        return &sockaddr_.in;
    }

    //! Cast the enclosed sockaddr into the sockaddr_in6 IPv6 structure.
    struct sockaddr_in6 * sockaddr_in6() {
        return &sockaddr_.in6;
    }

    //! Cast the enclosed sockaddr into the sockaddr_in6 IPv6 structure. Const
    //! version.
    const struct sockaddr_in6 * sockaddr_in6() const {
        return &sockaddr_.in6;
    }

    //! Return the enclosed socket address as a string without the port number.
    std::string ToStringHost() const;

    //! Return the enclosed socket address as a string with the port number.
    std::string ToStringHostPort() const;

    //! Make the socket address ostream-able: outputs address:port
    friend std::ostream& operator << (std::ostream& os,
                                      const SocketAddress& sa);

    //! Return the currently set port address in host byte-order.
    uint16_t GetPort() const {
        if (sockaddr()->sa_family == AF_INET) {
            return ntohs(sockaddr_in()->sin_port);
        }
        else if (sockaddr()->sa_family == AF_INET6) {
            return ntohs(sockaddr_in6()->sin6_port);
        }
        else
            return 0;
    }

    //! Change the currently set port address.
    void SetPort(uint16_t port) {
        if (sockaddr()->sa_family == AF_INET) {
            sockaddr_in()->sin_port = htons(port);
        }
        else if (sockaddr()->sa_family == AF_INET6) {
            sockaddr_in6()->sin6_port = htons(port);
        }
    }

    //! Resolve the given host name using getaddrinfo() and replace this object
    //! with the first socket address if found.
    bool Resolve(const char* hostname,
                 const char* servicename = nullptr);

    //! Resolve the given host name using getaddrinfo() and return only the
    //! first socket address if found.
    static SocketAddress ResolveOne(const char* hostname,
                                    const char* servicename = nullptr);

    //! Parse the address for a :port notation and then resolve the
    //! given host name using getaddrinfo() and return only the first
    //! socket address if found. Uses defaultservice if no port is
    //! found in the hostname.
    static SocketAddress ResolveWithPort(const char* hostname,
                                         const char* defaultservice);

    //! Resolve the given host name using getaddrinfo() and return all
    //! resulting socket addresses as a vector.
    static std::vector<SocketAddress>
    ResolveAll(const char* hostname, const char* servicename = nullptr);

    //! Return textual message of the last error occurring in the resolve
    //! method.
    const char * GetResolveError() const;
};

/*!
 * IPv4 Subclass of SocketAddress for direct initialization from a known IPv4
 * address, known either in binary format, numerals, or as "ddd.ddd.ddd.ddd"
 * format. No name lookup or resolution takes place in these functions.
 */
class IPv4Address : public SocketAddress
{
public:
    //! Create an IPv4 address and initialize only the port part.
    explicit IPv4Address(uint16_t port)
        : SocketAddress() {
        sockaddr_.in.sin_family = AF_INET;
        sockaddr_.in.sin_port = htons(port);
    }

    //! Create an IPv4 address object with initialized address and port parts.
    IPv4Address(uint32_t addr, uint16_t port)
        : SocketAddress() {
        sockaddr_.in.sin_family = AF_INET;
        sockaddr_.in.sin_addr.s_addr = addr;
        sockaddr_.in.sin_port = htons(port);
    }

    //! Create an IPv4 address object with initialized address and port parts.
    IPv4Address(struct in_addr& addr, uint16_t port)
        : SocketAddress() {
        sockaddr_.in.sin_family = AF_INET;
        sockaddr_.in.sin_addr = addr;
        sockaddr_.in.sin_port = htons(port);
    }

    //! Create an IPv4 address object and copy the given sockaddr_in structure.
    explicit IPv4Address(struct sockaddr_in& sa)
        : SocketAddress() {
        sockaddr_.in = sa;
    }

    //! Create an IPv4 address object and initialize it with the given ip
    //! address string, which is given in "ddd.ddd.ddd.ddd" format. You must
    //! check with IsValid() if the conversion was successfull.
    explicit IPv4Address(const char* ipstring, uint16_t port = 0);
};

/*!
 * IPv6 Subclass of SocketAddress for direct initialization from a known IPv6
 * address, known either in binary format, numerals, or in some IPv6 format. No
 * name lookup or resolution takes place in these functions.
 */
class IPv6Address : public SocketAddress
{
public:
    //! Create an IPv6 address and initialize only the port part.
    explicit IPv6Address(uint16_t port)
        : SocketAddress() {
        sockaddr_.in6.sin6_family = AF_INET6;
        sockaddr_.in6.sin6_port = htons(port);
    }

    //! Create an IPv6 address object with initialized address and port parts.
    IPv6Address(uint8_t addr[16], uint16_t port)
        : SocketAddress() {
        sockaddr_.in6.sin6_family = AF_INET6;
        memcpy(&sockaddr_.in6.sin6_addr, addr, 16 * sizeof(uint8_t));
        sockaddr_.in6.sin6_port = htons(port);
    }

    //! Create an IPv4 address object with initialized address and port parts.
    IPv6Address(struct in6_addr& addr, uint16_t port)
        : SocketAddress() {
        sockaddr_.in6.sin6_family = AF_INET6;
        sockaddr_.in6.sin6_addr = addr;
        sockaddr_.in6.sin6_port = htons(port);
    }

    //! Create an IPv4 address object and copy the given sockaddr_in structure.
    explicit IPv6Address(struct sockaddr_in6& sa)
        : SocketAddress() {
        sockaddr_.in6 = sa;
    }

    //! Create an IPv6 address object and initialize it with the given ip
    //! address string, which is given in some IPv6 format. You must check with
    //! IsValid() if the conversion was successfull.
    explicit IPv6Address(const char* ipstring, uint16_t port = 0);
};

// \}

} // namespace lowlevel
} // namespace net
} // namespace c7a

#endif // !C7A_NET_LOWLEVEL_SOCKET_ADDRESS_HEADER

/******************************************************************************/
