/*******************************************************************************
 * thrill/net/tcp/socket.cpp
 *
 * Lightweight wrapper around BSD socket API.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/net/tcp/socket.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

namespace thrill {
namespace net {
namespace tcp {

void Socket::SetKeepAlive(bool activate) {
    assert(IsValid());

    int sockoptflag = (activate ? 1 : 0);

    /* Enable sending of keep-alive messages on connection-oriented sockets. */
    if (::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE,
                     &sockoptflag, sizeof(sockoptflag)) != 0)
    {
        LOG << "Cannot set SO_KEEPALIVE on socket fd " << fd_
            << ": " << strerror(errno);
    }
}

void Socket::SetReuseAddr(bool activate) {
    assert(IsValid());

    int sockoptflag = (activate ? 1 : 0);

    /* set SO_REUSEPORT */
#ifdef SO_REUSEPORT
    if (::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT,
                     &sockoptflag, sizeof(sockoptflag)) != 0)
    {
        LOG << "Cannot set SO_REUSEPORT on socket fd " << fd_
            << ": " << strerror(errno);
    }
#else
    if (::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR,
                     &sockoptflag, sizeof(sockoptflag)) != 0)
    {
        LOG << "Cannot set SO_REUSEADDR on socket fd " << fd_
            << ": " << strerror(errno);
    }
#endif
}

void Socket::SetNoDelay(bool activate) {
    assert(IsValid());

#if __linux__ || __FreeBSD__ || __APPLE__
    int sockoptflag = (activate ? 1 : 0);

    /* TCP_NODELAY If set, disable the Nagle algorithm. This means that
       segments are always sent as soon as possible, even if there is only a
       small amount of data.  When not set, data is buffered until there is a
       sufficient amount to send out, thereby avoiding the frequent sending of
       small packets, which results in poor utilization of the network. This
       option cannot be used at the same time as the option TCP_CORK. */
    if (::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY,
                     &sockoptflag, sizeof(sockoptflag)) != 0)
    {
        LOG << "Cannot set TCP_NODELAY on socket fd " << fd_
            << ": " << strerror(errno);
    }
#endif
}

void Socket::SetSndBuf(size_t size) {
    assert(IsValid());

#if __linux__ || __FreeBSD__ || __APPLE__

    int sockoptflag = static_cast<int>(size);

    /*
     * SO_SNDBUF Sets or gets the maximum socket send buffer in bytes. The
     * kernel doubles this value (to allow space for bookkeeping overhead) when
     * it is set using setsockopt(2), and this doubled value is returned by
     * getsockopt(2). The default value is set by the
     * /proc/sys/net/core/wmem_default file and the maximum allowed value is set
     * by the /proc/sys/net/core/wmem_max file. The minimum (doubled) value for
     * this option is 2048.
     */
    if (::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF,
                     &sockoptflag, sizeof(sockoptflag)) != 0)
    {
        LOG << "Cannot set SO_SNDBUF on socket fd " << fd_
            << ": " << strerror(errno);
    }
#endif
}

void Socket::SetRcvBuf(size_t size) {
    assert(IsValid());

#if __linux__ || __FreeBSD__ || __APPLE__

    int sockoptflag = static_cast<int>(size);

    /*
     * SO_RCVBUF Sets or gets the maximum socket receive buffer in bytes. The
     * kernel doubles this value (to allow space for bookkeeping overhead) when
     * it is set using setsockopt(2), and this doubled value is returned by
     * getsockopt(2). The default value is set by the
     * /proc/sys/net/core/rmem_default file, and the maximum allowed value is
     * set by the /proc/sys/net/core/rmem_max file. The minimum (doubled) value
     * for this option is 256.
     */
    if (::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF,
                     &sockoptflag, sizeof(sockoptflag)) != 0)
    {
        LOG << "Cannot set SO_RCVBUF on socket fd " << fd_
            << ": " << strerror(errno);
    }
#endif
}

} // namespace tcp
} // namespace net
} // namespace thrill

/******************************************************************************/
