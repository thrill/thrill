/*******************************************************************************
 * c7a/net/lowlevel/socket.hpp
 *
 * Lightweight wrapper around BSD socket API.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_LOWLEVEL_SOCKET_HEADER
#define C7A_NET_LOWLEVEL_SOCKET_HEADER

#include <c7a/common/logger.hpp>
#include <c7a/common/string.hpp>
#include <c7a/net/lowlevel/socket_address.hpp>

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstring>
#include <utility>

namespace c7a {
namespace net {
namespace lowlevel {

//! \addtogroup netsock Low Level Socket API
//! \ingroup net
//! \{

/*!
 * Socket is a light-weight wrapper around the BSD socket API. Functions all
 * have plain return values and do not through exceptions.
 *
 * Not all functions in this class follow the normal naming conventions, because
 * they are wrappers around the equally named functions of the socket API.
 *
 * Sockets are currently copyable! One may want to add move semantics later.
 */
class Socket
{
    static const bool debug = false;

public:
    //! \name Creation
    //! \{

    //! Construct new Socket object from existing file descriptor.
    explicit Socket(int fd)
        : fd_(fd)
    { }

    Socket()
        : fd_(-1) { }

    //! Release this socket fd, make the Socket invalid.
    void Release() {
        fd_ = -1;
    }

    //! Create a new stream socket.
    static Socket Create() {
        int fd = ::socket(PF_INET, SOCK_STREAM, 0);

        if (fd < 0) {
            LOG << "Socket::Create()"
                << " fd=" << fd
                << " error=" << strerror(errno);
        }

        return Socket(fd);
    }

    //! Create a pair of connected stream sockets. Use this for internal local
    //! test connection pairs.
    static std::pair<Socket, Socket> CreatePair() {
        int fds[2];
        int r = ::socketpair(PF_UNIX, SOCK_STREAM, 0, fds);

        if (r != 0) {
            LOG << "Socket::CreatePair()"
                << " error=" << strerror(errno);
            abort();
        }

        return std::make_pair(Socket(fds[0]), Socket(fds[1]));
    }

    // Re-definition of standard socket errors.
    class Errors
    {
    public:
        // No-one listening on the remote address.
        static const int ConnectionRefused = ECONNREFUSED;
        // Timeout while attempting connection.
        static const int Timeout = ETIMEDOUT;
    };

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
    //! \return old blocking value (0 or 1) or -1 for error
    int SetNonBlocking(bool non_blocking) const {
        assert(IsValid());

        int old_opts = fcntl(fd_, F_GETFL);

        int new_opts = non_blocking
                       ? (old_opts | O_NONBLOCK) : (old_opts & ~O_NONBLOCK);

        if (fcntl(fd_, F_SETFL, new_opts) != 0)
        {
            LOG << "Socket::SetNonBlocking()"
                << " fd_=" << fd_
                << " non_blocking=" << non_blocking
                << " error=" << strerror(errno);
            return -1;
        }

        return old_opts;
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

    //! Shutdown one or both directions of socket.
    bool shutdown(int how = SHUT_RDWR) {
        assert(IsValid());

        if (::shutdown(fd_, how) != 0)
        {
            LOG << "Socket::shutdown()"
                << " fd_=" << fd_
                << " error=" << strerror(errno);
            return false;
        }

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

        return r;
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
        return r;
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
                << " data=" << common::hexdump(data, size)
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
                << " data=" << common::hexdump(data, size)
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
                << " data=" << common::hexdump(data, size)
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

    //! Send message to socket.
    ssize_t sendmsg(const struct msghdr* msg, int flags = 0) {
        assert(IsValid());

        if (debug) {
            SocketAddress msg_name(
                reinterpret_cast<struct sockaddr*>(msg->msg_name),
                msg->msg_namelen);

            LOG << "Socket::sendmsg()"
                << " fd_=" << fd_
                << " msg_name=" << msg_name
                << " iovec=" << iovec_tostring(msg->msg_iov, msg->msg_iovlen)
                << " control=" << common::hexdump(msg->msg_control, msg->msg_controllen)
                << " flags=" << flags;
        }

        ssize_t r = ::sendmsg(fd_, msg, flags);

        if (r < 0) {
            LOG << "error! Socket::send()"
                << " fd_=" << fd_
                << " return=" << r
                << " errno=" << strerror(errno);
        }

        return r;
    }

    //! Recv (outdata,maxsize) from socket (BSD socket API function wrapper)
    ssize_t recv_one(void* outdata, size_t maxsize, int flags = 0) {
        assert(IsValid());

        LOG << "Socket::recv_one()"
            << " fd_=" << fd_
            << " maxsize=" << maxsize
            << " flags=" << flags;

        ssize_t r = ::recv(fd_, outdata, maxsize, flags);

        if (debug) {
            LOG << "done Socket::recv_one()"
                << " fd_=" << fd_
                << " return=" << r
                << " data=" << (r >= 0 ? common::hexdump(outdata, r) : "<error>");
        }

        return r;
    }

    //! Receive (data,size) from socket, retry recvs if short-reads occur.
    ssize_t recv(void* outdata, size_t size, int flags = 0) {
        assert(IsValid());

        LOG << "Socket::recv()"
            << " fd_=" << fd_
            << " size=" << size
            << " flags=" << flags;

        char* cdata = static_cast<char*>(outdata);
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
                << " data=" << common::hexdump(outdata, rb);
        }

        return rb;
    }

    //! Recv (outdata,maxsize) and source address from socket (BSD socket API
    //! function wrapper)
    ssize_t recvfrom(void* outdata, size_t maxsize, int flags = 0,
                     SocketAddress* out_source = nullptr) {
        assert(IsValid());

        LOG << "Socket::recvfrom()"
            << " fd_=" << fd_
            << " maxsize=" << maxsize
            << " flags=" << flags
            << " out_socklen=" << (out_source ? out_source->socklen() : 0);

        socklen_t out_socklen = out_source ? out_source->socklen() : 0;

        ssize_t r = ::recvfrom(fd_, outdata, maxsize, flags,
                               out_source ? out_source->sockaddr() : nullptr,
                               &out_socklen);

        if (debug) {
            LOG << "done Socket::recvfrom()"
                << " fd_=" << fd_
                << " return=" << r
                << " data="
                << (r >= 0 ? common::hexdump(outdata, r) : "<error>")
                << " out_source="
                << (out_source ? out_source->ToStringHostPort() : "<null>");
        }

        return r;
    }

    //! Send message to socket.
    ssize_t recvmsg(struct msghdr* msg, int flags = 0) {
        assert(IsValid());

        if (debug) {
            SocketAddress msg_name(
                reinterpret_cast<struct sockaddr*>(msg->msg_name),
                msg->msg_namelen);

            LOG << "Socket::recvmsg()"
                << " fd_=" << fd_
                << " msg_name=" << msg_name
                << " iovec=" << iovec_tostring(msg->msg_iov, msg->msg_iovlen)
                << " control=" << common::hexdump(msg->msg_control, msg->msg_controllen)
                << " flags=" << flags;
        }

        ssize_t r = ::recvmsg(fd_, msg, flags);

        if (r < 0) {
            LOG << "error! Socket::send()"
                << " fd_=" << fd_
                << " return=" << r
                << " errno=" << strerror(errno);
        }
        else {
            SocketAddress msg_name(
                reinterpret_cast<struct sockaddr*>(msg->msg_name),
                msg->msg_namelen);

            LOG << "Socket::recvmsg()"
                << " fd_=" << fd_
                << " msg_name=" << msg_name
                << " iovec=" << iovec_tostring(msg->msg_iov, msg->msg_iovlen)
                << " control=" << common::hexdump(msg->msg_control, msg->msg_controllen)
                << " flags=" << flags;
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

    //! \}

    //! Output a list of scattered iovec vectors for debugging.
    static inline
    std::string iovec_tostring(struct iovec* iov, size_t iovlen) {
        if (iovlen == 0)
            return "[empty]";
        if (iovlen == 1)
            return common::hexdump(iov[0].iov_base, iov[0].iov_len);

        std::ostringstream oss;
        oss << '[';
        for (size_t i = 0; i < iovlen; ++i) {
            if (i != 0) oss << ',';
            oss << common::hexdump(iov[i].iov_base, iov[i].iov_len);
        }
        oss << ']';
        return oss.str();
    }

protected:
    //! the file descriptor of the socket.
    int fd_;
};

// \}

} // namespace lowlevel
} // namespace net
} // namespace c7a

#endif // !C7A_NET_LOWLEVEL_SOCKET_HEADER

/******************************************************************************/
