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

    ssize_t send(const void *data, size_t size, int flags = 0)
    {
        LOG << "Socket(" << fd_ << ")::send() data=" << "size=" << size;
        LOG << "data = " << hexdump(data, size);
        ssize_t r = ::send(fd_, data, size, flags);
        LOG << "Socket(" << fd_ << ")::send() return=" << r;
        return r;
    }
    
    ssize_t recv(void* outdata, size_t maxsize, int flags = 0)
    {
        LOG << "Socket(" << fd_ << ")::recv() flags=" << flags;
        ssize_t r = ::recv(fd_, outdata, maxsize, flags);
        LOG << "Socket(" << fd_ << ")::recv() returned " << r;
        return r;
    }
    
protected:

    int fd_;
};

} // namespace c7a

#endif // !C7A_NEW_HEADER

/******************************************************************************/
