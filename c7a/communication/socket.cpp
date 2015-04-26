/*******************************************************************************
 * c7a/communication/socket.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/communication/socket.hpp>

#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

namespace c7a {

void Socket::set_keepalive(bool activate)
{
    int sockoptflag = (activate ? 1 : 0);

    /* Enable sending of keep-alive messages on connection-oriented sockets. */
    if (setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE,
                   &sockoptflag, sizeof(sockoptflag)) != 0)
    {
        LOG << "Cannot set SO_KEEPALIVE on socket fd " << fd_
            << ": " << strerror(errno);
    }
}

void Socket::set_reuseaddr(bool activate)
{
    int sockoptflag = (activate ? 1 : 0);

    /* set SO_REUSEPORT */
#ifdef SO_REUSEPORT
    if (setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT,
                   &sockoptflag, sizeof(sockoptflag)) != 0)
    {
        LOG << "Cannot set SO_REUSEPORT on socket fd " << fd_
            << ": " << strerror(errno);
    }
#else
    if (setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR,
                   &sockoptflag, sizeof(sockoptflag)) != 0)
    {
        LOG << "Cannot set SO_REUSEADDR on socket fd " << fd_
            << ": " << strerror(errno);
    }
#endif
}

void Socket::set_nodelay(bool activate)
{
    int sockoptflag = (activate ? 1 : 0);

#if __linux__ || __FreeBSD__
    /* TCP_NODELAY If set, disable the Nagle algorithm. This means that
       segments are always sent as soon as possible, even if there is only a
       small amount of data.  When not set, data is buffered until there is a
       sufficient amount to send out, thereby avoiding the frequent sending of
       small packets, which results in poor utilization of the network. This
       option cannot be used at the same time as the option TCP_CORK. */
    if (setsockopt(fd_, SOL_TCP, TCP_NODELAY,
                   &sockoptflag, sizeof(sockoptflag)) != 0)
    {
        LOG << "Cannot set TCP_NODELAY on socket fd " << fd_
            << ": " << strerror(errno);
    }
#endif
}

} // namespace c7a

/******************************************************************************/
