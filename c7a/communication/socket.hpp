/*******************************************************************************
 * c7a/communication/socket.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_COMMUNICATION_SOCKET_HEADER
#define C7A_COMMUNICATION_SOCKET_HEADER

#include <c7a/common/logger.hpp>
#include <c7a/common/string.hpp>
#include <c7a/communication/socket_address.hpp>

#include <cerrno>
#include <cstring>
#include <cassert>

#include <sys/socket.h>

namespace c7a {

class Socket
{
    static const bool debug = true;

public:
    explicit Socket(int fd = -1)
        : fd_(fd)
    { }

    //! Create a new stream socket.
    static Socket Create()
    {
        int fd = socket(AF_INET, SOCK_STREAM, 0);

        if (fd < 0) {
            LOG << "Socket::Create()"
                << " fd=" << fd
                << " error=" << strerror(errno);
        }

        return Socket(fd);
    }

    //! Check whether the contained file descriptor is valid.
    bool IsValid() const { return fd_ >= 0; }

    int GetFileDescriptor() const
    {
        return fd_;
    }

    //! \name Connect, Bind and Accept Functions
    //! \{

    //! Bind socket to given SocketAddress for listening or connecting.
    bool bind(const SocketAddress& sa)
    {
        if (::bind(fd_, sa.sockaddr(), sa.get_socklen()) != 0)
        {
            LOG << "Socket::bind()"
                << " fd_=" << fd_
                << " sa=" << sa
                << " error=" << strerror(errno);
            return false;
        }

        return true;
    }

    //! Initial socket connection to address
    bool connect(const SocketAddress& sa)
    {
        int r = ::connect(fd_, sa.sockaddr(), sa.get_socklen());

        if (r == 0) {
            is_connected_ = true;
            return true;
        }

        LOG << "Socket::connect()"
            << " fd_=" << fd_
            << " sa=" << sa
            << " error=" << strerror(errno);

        return false;
    }

    //! Turn socket into listener state to accept incoming connections.
    int listen(int backlog = 0)
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
        return (r == 0);
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
    ssize_t _send(const void* data, size_t size, int flags = 0)
    {
        LOG << "Socket::_send()"
            << " fd_=" << fd_
            << " size=" << size
            << " data=" << hexdump(data, size)
            << " flags=" << flags;

        ssize_t r = ::send(fd_, data, size, flags);

        LOG << "done Socket::_send()"
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
    ssize_t _recv(void* outdata, size_t maxsize, int flags = 0)
    {
        LOG << "Socket::_recv()"
            << " fd_=" << fd_
            << " maxsize=" << maxsize
            << " flags=" << flags;

        ssize_t r = ::recv(fd_, outdata, maxsize, flags);

        LOG << "done Socket::_recv()"
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

    //! \name Accelerations
    //! \{

    //! Enable sending of keep-alive messages on connection-oriented sockets.
    void set_keepalive(bool activate = true);

    //! Enable SO_REUSEADDR, which allows the socket to be bound more quickly to
    //! previously used ports.
    void set_reuseaddr(bool activate = true);

    //! If set, disable the Nagle algorithm. This means that segments are always
    //! sent as soon as possible, even if there is only a small amount of data.
    void set_nodelay(bool activate = true);

    //! \}

protected:
    //! the file descriptor of the socket.
    int fd_;

    //! check flag that the socket was turned into listen state
    bool is_listensocket_ = false;

    //! flag whether the socket was connected
    bool is_connected_ = false;
};

} // namespace c7a

#endif // !C7A_COMMUNICATION_SOCKET_HEADER

/******************************************************************************/
