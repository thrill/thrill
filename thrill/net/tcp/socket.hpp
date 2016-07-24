/*******************************************************************************
 * thrill/net/tcp/socket.hpp
 *
 * Lightweight wrapper around BSD socket API.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_TCP_SOCKET_HEADER
#define THRILL_NET_TCP_SOCKET_HEADER

#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/net/tcp/socket_address.hpp>

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

namespace thrill {
namespace net {
namespace tcp {

//! \addtogroup net_tcp TCP Socket API
//! \ingroup net
//! \{

/*!
 * Socket is a light-weight wrapper around the BSD socket API. Functions all
 * have plain return values and do not through exceptions.
 *
 * Not all functions in this class follow the normal naming conventions, because
 * they are wrappers around the equally named functions of the socket API.
 */
class Socket
{
    static constexpr bool debug = false;
    static constexpr bool debug_data = false;

public:
    //! \name Creation
    //! \{

    //! construct new Socket object from existing file descriptor.
    explicit Socket(int fd)
        : fd_(fd) {
        SetNoDelay(true);
        SetSndBuf(2 * 1024 * 1024);
        SetRcvBuf(2 * 1024 * 1024);
    }

    //! default constructor: invalid socket.
    Socket() : fd_(-1) { }

    //! non-copyable: delete copy-constructor
    Socket(const Socket&) = delete;
    //! non-copyable: delete assignment operator
    Socket& operator = (const Socket&) = delete;
    //! move-constructor: move file descriptor
    Socket(Socket&& s) noexcept : fd_(s.fd_) { s.fd_ = -1; }
    //! move-assignment operator: move file descriptor
    Socket& operator = (Socket&& s) {
        if (this == &s) return *this;
        if (fd_ >= 0) close();
        fd_ = s.fd_;
        s.fd_ = -1;
        return *this;
    }

    ~Socket() {
        if (fd_ >= 0) close();
    }

    //! Create a new stream socket.
    static Socket Create() {
#ifdef SOCK_CLOEXEC
        int fd = ::socket(PF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
#else
        int fd = ::socket(PF_INET, SOCK_STREAM, 0);
#endif
        if (fd < 0) {
            LOG << "Socket::Create()"
                << " fd=" << fd
                << " error=" << strerror(errno);
        }

#ifndef SOCK_CLOEXEC
        if (fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) {
            throw common::ErrnoException(
                      "Error setting FD_CLOEXEC on network socket");
        }
#endif

        return Socket(fd);
    }

    //! Create a pair of connected stream sockets. Use this for internal local
    //! test connection pairs.
    static std::pair<Socket, Socket> CreatePair() {
        int fds[2];
#ifdef SOCK_CLOEXEC
        int r = ::socketpair(PF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds);
#else
        int r = ::socketpair(PF_UNIX, SOCK_STREAM, 0, fds);
#endif
        if (r != 0) {
            LOG1 << "Socket::CreatePair()"
                 << " error=" << strerror(errno);
            abort();
        }

#ifndef SOCK_CLOEXEC
        if (fcntl(fds[0], F_SETFD, FD_CLOEXEC) != 0) {
            throw common::ErrnoException(
                      "Error setting FD_CLOEXEC on network socket");
        }
        if (fcntl(fds[1], F_SETFD, FD_CLOEXEC) != 0) {
            throw common::ErrnoException(
                      "Error setting FD_CLOEXEC on network socket");
        }
#endif
        return std::make_pair(Socket(fds[0]), Socket(fds[1]));
    }

    //! \}

    //! \name Status
    //! \{

    //! Check whether the contained file descriptor is valid.
    bool IsValid() const
    { return fd_ >= 0; }

    //! Return the associated file descriptor
    int fd() const { return fd_; }

    //! Query socket for its current error state.
    int GetError() const {
        int socket_error = -1;
        socklen_t len = sizeof(socket_error);
        getsockopt(SOL_SOCKET, SO_ERROR, &socket_error, &len);
        return socket_error;
    }

    //! Turn socket into non-blocking state.
    bool SetNonBlocking(bool non_blocking) {
        assert(IsValid());

        if (non_blocking == non_blocking_) return true;

        int old_opts = fcntl(fd_, F_GETFL);

        int new_opts = non_blocking
                       ? (old_opts | O_NONBLOCK) : (old_opts & ~O_NONBLOCK);

        if (fcntl(fd_, F_SETFL, new_opts) != 0)
        {
            LOG << "Socket::SetNonBlocking()"
                << " fd_=" << fd_
                << " non_blocking=" << non_blocking
                << " error=" << strerror(errno);
            return false;
        }

        non_blocking_ = non_blocking;
        return true;
    }

    //! Return the current local socket address.
    SocketAddress GetLocalAddress() const {
        assert(IsValid());

        struct sockaddr_in6 sa;
        socklen_t salen = sizeof(sa);

        if (getsockname(
                fd_, reinterpret_cast<struct sockaddr*>(&sa), &salen) != 0)
        {
            LOG << "Socket::GetLocalAddress()"
                << " fd_=" << fd_
                << " error=" << strerror(errno);
            return SocketAddress();
        }

        return SocketAddress(reinterpret_cast<struct sockaddr*>(&sa), salen);
    }

    //! Return the current peer socket address.
    SocketAddress GetPeerAddress() const {
        assert(IsValid());

        struct sockaddr_in6 sa;
        socklen_t salen = sizeof(sa);

        if (getpeername(
                fd_, reinterpret_cast<struct sockaddr*>(&sa), &salen) != 0)
        {
            LOG << "Socket::GetPeerAddress()"
                << " fd_=" << fd_
                << " error=" << strerror(errno);
            return SocketAddress();
        }

        return SocketAddress(reinterpret_cast<struct sockaddr*>(&sa), salen);
    }

    //! \}

    //! \name Close
    //! \{

    //! Close socket.
    bool close() {
        assert(IsValid());

        if (::close(fd_) != 0)
        {
            LOG << "Socket::close()"
                << " fd_=" << fd_
                << " error=" << strerror(errno);
            return false;
        }

        LOG << "Socket::close()"
            << " fd_=" << fd_
            << " closed";

        fd_ = -1;

        return true;
    }

    //! \}

    //! \name Connect, Bind and Accept Functions
    //! \{

    //! Bind socket to given SocketAddress for listening or connecting.
    bool bind(const SocketAddress& sa) {
        assert(IsValid());
        assert(sa.IsValid());

        int r = ::bind(fd_, sa.sockaddr(), sa.socklen());

        if (r != 0) {
            LOG << "Socket::bind()"
                << " fd_=" << fd_
                << " sa=" << sa
                << " return=" << r
                << " error=" << strerror(errno);
        }

        return (r == 0);
    }

    //! Initial socket connection to address
    int connect(const SocketAddress& sa) {
        assert(IsValid());
        assert(sa.IsValid());

        int r = ::connect(fd_, sa.sockaddr(), sa.socklen());

        if (r == 0)
            return r;

        LOG << "Socket::connect()"
            << " fd_=" << fd_
            << " sa=" << sa
            << " return=" << r
            << " error=" << strerror(errno);

        return r;
    }

    //! Turn socket into listener state to accept incoming connections.
    bool listen(int backlog = 0) {
        assert(IsValid());

        if (backlog == 0) backlog = SOMAXCONN;

        int r = ::listen(fd_, backlog);

        if (r == 0) {
            LOG << "Socket::listen()"
                << " fd_=" << fd_;
        }
        else {
            LOG << "Socket::listen()"
                << " fd_=" << fd_
                << " error=" << strerror(errno);
        }
        return (r == 0);
    }

    //! Wait on socket until a new connection comes in.
    Socket accept() const {
        assert(IsValid());

        struct sockaddr_in6 sa;
        socklen_t salen = sizeof(sa);

        int newfd = ::accept(fd_,
                             reinterpret_cast<struct sockaddr*>(&sa), &salen);
        if (newfd < 0) {
            LOG << "Socket::accept()"
                << " fd_=" << fd_
                << " error=" << strerror(errno);
            return Socket();
        }

        LOG << "Socket::accept()"
            << " fd_=" << fd_
            << " newfd=" << newfd;

        return Socket(newfd);
    }

    //! \}

    //! \name Send and Recv Functions
    //! \{

    //! Send (data,size) to socket (BSD socket API function wrapper), for
    //! blocking sockets one should probably use send() instead of this
    //! lower-layer functions.
    ssize_t send_one(const void* data, size_t size, int flags = 0) {
        assert(IsValid());

        if (debug) {
            LOG << "Socket::send_one()"
                << " fd_=" << fd_
                << " size=" << size
                << " data=" << MaybeHexdump(data, size)
                << " flags=" << flags;
        }

        ssize_t r = ::send(fd_, data, size, flags);

        LOG << "done Socket::send_one()"
            << " fd_=" << fd_
            << " return=" << r;

        return r;
    }

    //! Send (data,size) to socket, retry sends if short-sends occur.
    ssize_t send(const void* data, size_t size, int flags = 0) {
        assert(IsValid());

        if (debug) {
            LOG << "Socket::send()"
                << " fd_=" << fd_
                << " size=" << size
                << " data=" << MaybeHexdump(data, size)
                << " flags=" << flags;
        }

        const char* cdata = static_cast<const char*>(data);
        size_t wb = 0; // written bytes

        while (wb < size)
        {
            ssize_t r = ::send(fd_, cdata + wb, size - wb, flags);

            if (r <= 0) {
                // an error occured, check errno.
                if (errno == EAGAIN) continue;

                LOG << "done Socket::send()"
                    << " fd_=" << fd_
                    << " return=" << r
                    << " errno=" << strerror(errno);

                return r;
            }

            wb += r;
        }

        LOG << "done Socket::send()"
            << " fd_=" << fd_
            << " return=" << wb;

        return wb;
    }

    //! Send (data,size) to destination
    ssize_t sendto(const void* data, size_t size, int flags,
                   const SocketAddress& dest) {
        assert(IsValid());

        if (debug) {
            LOG << "Socket::sendto()"
                << " fd_=" << fd_
                << " size=" << size
                << " data=" << MaybeHexdump(data, size)
                << " flags=" << flags
                << " dest=" << dest;
        }

        ssize_t r = ::sendto(fd_, data, size, flags,
                             dest.sockaddr(), dest.socklen());

        LOG << "done Socket::sendto()"
            << " fd_=" << fd_
            << " return=" << r;

        return r;
    }

    //! Recv (out_data,max_size) from socket (BSD socket API function wrapper)
    ssize_t recv_one(void* out_data, size_t max_size, int flags = 0) {
        assert(IsValid());

        // this is a work-around, since on errno is spontaneously == EINVAL,
        // with no relationship to recv() -tb 2015-08-28
        errno = 0;

        LOG << "Socket::recv_one()"
            << " fd_=" << fd_
            << " max_size=" << max_size
            << " flags=" << flags
            << " errno=" << errno;

        ssize_t r = ::recv(fd_, out_data, max_size, flags);

        if (debug) {
            LOG << "done Socket::recv_one()"
                << " fd_=" << fd_
                << " return=" << r
                << " errno=" << errno
                << " data=" << (r >= 0 ? MaybeHexdump(out_data, r) : "<error>");
        }

        return r;
    }

    //! Receive (data,size) from socket, retry recvs if short-reads occur.
    ssize_t recv(void* out_data, size_t size, int flags = 0) {
        assert(IsValid());

        LOG << "Socket::recv()"
            << " fd_=" << fd_
            << " size=" << size
            << " flags=" << flags;

        char* cdata = static_cast<char*>(out_data);
        size_t rb = 0; // read bytes

        while (rb < size)
        {
            ssize_t r = ::recv(fd_, cdata + rb, size - rb, flags);

            if (r <= 0) {
                // an error occured, check errno.
                if (errno == EAGAIN) continue;

                LOG << "done Socket::recv()"
                    << " fd_=" << fd_
                    << " size=" << size
                    << " return=" << r
                    << " errno=" << strerror(errno);

                return r;
            }

            rb += r;
        }

        if (debug) {
            LOG << "done Socket::recv()"
                << " fd_=" << fd_
                << " return=" << rb
                << " data=" << MaybeHexdump(out_data, rb);
        }

        return rb;
    }

    //! Recv (out_data,max_size) and source address from socket (BSD socket API
    //! function wrapper)
    ssize_t recvfrom(void* out_data, size_t max_size, int flags = 0,
                     SocketAddress* out_source = nullptr) {
        assert(IsValid());

        LOG << "Socket::recvfrom()"
            << " fd_=" << fd_
            << " max_size=" << max_size
            << " flags=" << flags
            << " out_socklen=" << (out_source ? out_source->socklen() : 0);

        socklen_t out_socklen = out_source ? out_source->socklen() : 0;

        ssize_t r = ::recvfrom(fd_, out_data, max_size, flags,
                               out_source ? out_source->sockaddr() : nullptr,
                               &out_socklen);

        if (debug) {
            LOG << "done Socket::recvfrom()"
                << " fd_=" << fd_
                << " return=" << r
                << " data="
                << (r >= 0 ? MaybeHexdump(out_data, r) : "<error>")
                << " out_source="
                << (out_source ? out_source->ToStringHostPort() : "<null>");
        }

        return r;
    }

    //! \}

    //! \name Socket Options and Accelerations
    //! \{

    //! Perform raw getsockopt() operation on socket.
    int getsockopt(int level, int optname,
                   void* optval, socklen_t* optlen) const {
        assert(IsValid());

        int r = ::getsockopt(fd_, level, optname, optval, optlen);

        if (r != 0)
            LOG << "Socket::getsockopt()"
                << " fd_=" << fd_
                << " level=" << level
                << " optname=" << optname
                << " optval=" << optval
                << " optlen=" << optlen
                << " error=" << strerror(errno);

        return r;
    }

    //! Perform raw setsockopt() operation on socket.
    int setsockopt(int level, int optname,
                   const void* optval, socklen_t optlen) {
        assert(IsValid());

        int r = ::setsockopt(fd_, level, optname, optval, optlen);

        if (r != 0)
            LOG << "Socket::setsockopt()"
                << " fd_=" << fd_
                << " level=" << level
                << " optname=" << optname
                << " optval=" << optval
                << " optlen=" << optlen
                << " error=" << strerror(errno);

        return r;
    }

    //! Enable sending of keep-alive messages on connection-oriented sockets.
    void SetKeepAlive(bool activate = true);

    //! Enable SO_REUSEADDR, which allows the socket to be bound more quickly to
    //! previously used ports.
    void SetReuseAddr(bool activate = true);

    //! If set, disable the Nagle algorithm. This means that segments are always
    //! sent as soon as possible, even if there is only a small amount of data.
    void SetNoDelay(bool activate = true);

    //! Set SO_SNDBUF socket option.
    void SetSndBuf(size_t size);

    //! Set SO_RCVBUF socket option.
    void SetRcvBuf(size_t size);

    //! \}

private:
    //! the file descriptor of the socket.
    int fd_;

    //! flag whether the socket is set to non-blocking
    bool non_blocking_ = false;

    //! return hexdump or just [data] if not debugging
    static std::string MaybeHexdump(const void* data, size_t size) {
        if (debug_data)
            return common::Hexdump(data, size);
        else
            return "[data]";
    }
};

// \}

} // namespace tcp
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_TCP_SOCKET_HEADER

/******************************************************************************/
