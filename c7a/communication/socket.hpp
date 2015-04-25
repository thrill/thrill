/*******************************************************************************
 * c7a/<file>
 *
 *
 ******************************************************************************/

#ifndef C7A_NEW_HEADER
#define C7A_NEW_HEADER

#include <c7a/common/logger.hpp>
#include <c7a/common/string.hpp>

namespace c7a {

class Socket
{
    static const bool debug = true;

public:
    Socket(int fd = -1)
        : fd_(fd)
    {
    }

    int GetFileDescriptor() const
    {
        return fd_;
    }

    //! Send (data,size) to socket (BSD socket API function wrapper), for
    //! blocking sockets one should probably use send() instead of this
    //! lower-layer functions.
    ssize_t _send(const void *data, size_t size, int flags = 0)
    {
        LOG << "Socket::send()"
            << " fd_=" << fd_
            << " size=" << size
            << " data=" << hexdump(data, size)
            << " flags=" << flags;

        ssize_t r = ::send(fd_, data, size, flags);

        LOG << "done Socket::send()"
            << " fd_=" << fd_
            << " return=" << r;

        return r;
    }

    //! Send (data,size) to socket, retry sends if short-sends occur.
    ssize_t send(const void *data, size_t size, int flags = 0)
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
    ssize_t recv(void* outdata, size_t maxsize, int flags = 0)
    {
        LOG << "Socket::recv()"
            << " fd_=" << fd_
            << " maxsize=" << maxsize
            << " flags=" << flags;

        ssize_t r = ::recv(fd_, outdata, maxsize, flags);

        LOG << "done Socket::recv()"
            << " fd_=" << fd_
            << " return=" << r
            << " data=" << (r >= 0 ? hexdump(outdata, r) : "<error>");

        return r;
    }

protected:

    int fd_;
};

} // namespace c7a

#endif // !C7A_NEW_HEADER

/******************************************************************************/
