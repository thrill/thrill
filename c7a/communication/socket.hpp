/*******************************************************************************
 * c7a/communication/socket.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#ifndef C7A_COMMUNICATION_SOCKET_HEADER
#define C7A_COMMUNICATION_SOCKET_HEADER

#include <c7a/common/logger.hpp>
#include <c7a/common/string.hpp>

#include <cerrno>
#include <cstring>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

namespace c7a {

class Socket
{
    static const bool debug = true;

public:
    explicit Socket(int fd = -1)
        : fd_(fd)
    { }

    int GetFileDescriptor() const
    {
        return fd_;
    }

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
};

} // namespace c7a

#endif // !C7A_COMMUNICATION_SOCKET_HEADER

/******************************************************************************/
