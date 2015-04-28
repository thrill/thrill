/*******************************************************************************
 * c7a/net/socket.hpp
 *
 * Lightweight wrapper around BSD socket API.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#ifndef C7A_NET_SOCKET_HEADER
#define C7A_NET_SOCKET_HEADER

#include <c7a/common/logger.hpp>
#include <c7a/common/string.hpp>
#include <c7a/net/socket-address.hpp>

#include <cerrno>
#include <cstring>
#include <cassert>

#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>

namespace c7a {

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
    static const bool debug = true;

public:
    //! \name Creation
    //! \{

    //! Construct new Socket object from existing file descriptor.
    explicit Socket(int fd = -1)
        : fd_(fd)
    { }

    //! Create a new stream socket.
    static Socket Create()
    {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);

        if (fd < 0) {
            LOG << "Socket::Create()"
                << " fd=" << fd
                << " error=" << strerror(errno);
        }

        return Socket(fd);
    }

    //! Create a pair of connected stream sockets. Use this for internal local
    //! test connection pairs.
    static std::pair<Socket, Socket> CreatePair()
    {
        int fds[2];
        int r = socketpair(PF_UNIX, SOCK_STREAM, 0, fds);

        if (r != 0) {
            LOG << "Socket::CreatePair()"
                << " error=" << strerror(errno);
            abort();
        }

        return std::make_pair(Socket(fds[0]), Socket(fds[1]));
    }

    //! \}

    //! \name Status
    //! \{

    //! Check whether the contained file descriptor is valid.
    bool IsValid() const
    { return fd_ >= 0; }

    //! Return the associated file descriptor
    int GetFileDescriptor() const
    { return fd_; }

    //! Query socket for its current error state.
    int GetError() const
    {
        int socket_error;
        socklen_t len = sizeof(socket_error);
        getsockopt(SOL_SOCKET, SO_ERROR, &socket_error, &len);
        return socket_error;
    }

    //! Turn socket into non-blocking state.
    //! \return old blocking value (0 or 1) or -1 for error
    int SetNonBlocking(bool non_blocking)
    {
        if (non_blocking == non_blocking_) return 1;

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

        non_blocking_ = non_blocking;
        return old_opts;
    }

    //! Return the current local socket address.
    SocketAddress GetLocalAddress()
    {
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
    SocketAddress GetPeerAddress()
    {
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
    bool close()
    {
        if (::close(fd_) != 0)
        {
            LOG << "Socket::close()"
                << " fd_=" << fd_
                << " error=" << strerror(errno);
            return false;
        }

        return true;
    }

    //! Shutdown one or both directions of socket.
    bool shutdown(int how = SHUT_RDWR)
    {
        if (::shutdown(fd_, how) != 0)
        {
            LOG << "Socket::shutdown()"
                << " fd_=" << fd_
                << " error=" << strerror(errno);
            return false;
        }

        return true;
    }

    //! \}

    //! \name Connect, Bind and Accept Functions
    //! \{

    //! Bind socket to given SocketAddress for listening or connecting.
    bool bind(const SocketAddress& sa)
    {
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
    int connect(const SocketAddress& sa)
    {
        int r = ::connect(fd_, sa.sockaddr(), sa.socklen());

        if (r == 0) {
            is_connected_ = true;
            return r;
        }

        LOG << "Socket::connect()"
            << " fd_=" << fd_
            << " sa=" << sa
            << " return=" << r
            << " error=" << strerror(errno);

        return r;
    }

    //! Turn socket into listener state to accept incoming connections.
    bool listen(int backlog = 0)
    {
        if (backlog == 0) backlog = SOMAXCONN;

        int r = ::listen(fd_, backlog);

        if (r == 0) {
            is_listensocket_ = 1;
        }
        else {
            LOG << "Socket::listen()"
                << " fd_=" << fd_
                << " error=" << strerror(errno);
        }
        return r;
    }

    //! Wait on socket until a new connection comes in.
    Socket accept()
    {
        assert(is_listensocket_);

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
    ssize_t send_one(const void* data, size_t size, int flags = 0)
    {
        LOG << "Socket::send_one()"
            << " fd_=" << fd_
            << " size=" << size
            << " data=" << hexdump(data, size)
            << " flags=" << flags;

        ssize_t r = ::send(fd_, data, size, flags);

        LOG << "done Socket::send_one()"
            << " fd_=" << fd_
            << " return=" << r;

        return r;
    }

    //! Send (data,size) to socket, retry sends if short-sends occur.
    ssize_t send(const void* data, size_t size, int flags = 0)
    {
        LOG << "Socket::send()"
            << " fd_=" << fd_
            << " size=" << size
            << " data=" << hexdump(data, size)
            << " flags=" << flags;

        const char* cdata = static_cast<const char*>(data);
        size_t wb = 0; // written bytes

        while (wb < size)
        {
            ssize_t r = ::send(fd_, cdata + wb, size - wb, flags);

            if (r < 0) {
                // an error occured, check errno.

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

    //! Recv (outdata,maxsize) from socket (BSD socket API function wrapper)
    ssize_t recv_one(void* outdata, size_t maxsize, int flags = 0)
    {
        LOG << "Socket::recv_one()"
            << " fd_=" << fd_
            << " maxsize=" << maxsize
            << " flags=" << flags;

        ssize_t r = ::recv(fd_, outdata, maxsize, flags);

        LOG << "done Socket::recv_one()"
            << " fd_=" << fd_
            << " return=" << r
            << " data=" << (r >= 0 ? hexdump(outdata, r) : "<error>");

        return r;
    }

    //! Receive (data,size) from socket, retry recvs if short-reads occur.
    ssize_t recv(void* outdata, size_t size, int flags = 0)
    {
        LOG << "Socket::recv()"
            << " fd_=" << fd_
            << " size=" << size
            << " flags=" << flags;

        char* cdata = static_cast<char*>(outdata);
        size_t rb = 0; // read bytes

        while (rb < size)
        {
            ssize_t r = ::recv(fd_, cdata + rb, size - rb, flags);

            if (r < 0) {
                // an error occured, check errno.

                LOG << "done Socket::recv()"
                    << " fd_=" << fd_
                    << " size=" << size
                    << " return=" << r
                    << " errno=" << strerror(errno);

                return r;
            }

            rb += r;
        }

        LOG << "done Socket::recv()"
            << " fd_=" << fd_
            << " return=" << rb
            << " data=" << hexdump(outdata, rb);

        return rb;
    }

    //! \}

    //! \name Socket Options and Accelerations
    //! \{

    //! Perform raw getsockopt() operation on socket.
    int getsockopt(int level, int optname,
                   void* optval, socklen_t* optlen) const
    {
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
                   const void* optval, socklen_t optlen)
    {
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

protected:
    //! the file descriptor of the socket.
    int fd_;

    //! check flag that the socket was turned into listen state
    bool is_listensocket_ = false;

    //! flag whether the socket was connected
    bool is_connected_ = false;

    //! flag whether the socket is set to non-blocking mode
    bool non_blocking_ = false;
};

// \}

} // namespace c7a

#endif // !C7A_NET_SOCKET_HEADER

/******************************************************************************/
