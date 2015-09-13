/*******************************************************************************
 * thrill/net/tcp/select.hpp
 *
 * Lightweight wrapper around select()
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_TCP_SELECT_HEADER
#define THRILL_NET_TCP_SELECT_HEADER

#include <sys/select.h>

#include <algorithm>
#include <cassert>
#include <limits>

namespace thrill {
namespace net {
namespace tcp {

//! \addtogroup net_tcp TCP Socket API
//! \{

/**
 * Select is an object-oriented wrapper for select(). It takes care of the
 * bit-fields, etc.
 */
class Select
{
public:
    //! constructor
    Select() {
        FD_ZERO(&read_set_);
        FD_ZERO(&write_set_);
        FD_ZERO(&except_set_);
    }

    //! Add a socket to the read and exception selection set
    Select & SetRead(int fd) {
        assert(fd >= 0);
        FD_SET(fd, &read_set_);
        max_fd_ = std::max(max_fd_, fd);
        return *this;
    }

    //! Add a socket to the write and exception selection set
    Select & SetWrite(int fd) {
        assert(fd >= 0);
        FD_SET(fd, &write_set_);
        max_fd_ = std::max(max_fd_, fd);
        return *this;
    }

    //! Add a socket to the exception selection set
    Select & SetException(int fd) {
        assert(fd >= 0);
        FD_SET(fd, &except_set_);
        max_fd_ = std::max(max_fd_, fd);
        return *this;
    }

    //! Check if a file descriptor is in the resulting read set.
    bool InRead(int fd) const
    { return FD_ISSET(fd, &read_set_) != 0; }

    //! Check if a file descriptor is in the resulting Write set.
    bool InWrite(int fd) const
    { return FD_ISSET(fd, &write_set_) != 0; }

    //! Check if a file descriptor is in the resulting exception set.
    bool InException(int fd) const
    { return FD_ISSET(fd, &except_set_) != 0; }

    //! Clear a file descriptor from the read set
    Select & ClearRead(int fd)
    { assert(fd >= 0); FD_CLR(fd, &read_set_); return *this; }

    //! Clear a file descriptor from the write set
    Select & ClearWrite(int fd)
    { assert(fd >= 0); FD_CLR(fd, &write_set_); return *this; }

    //! Clear a file descriptor from the exception set
    Select & ClearException(int fd)
    { assert(fd >= 0); FD_CLR(fd, &except_set_); return *this; }

    //! Clear a file descriptor from all sets
    Select & Clear(int fd)
    { return ClearRead(fd).ClearWrite(fd).ClearException(fd); }

    //! Do a select(), which modifies the enclosed file descriptor objects.
    int select(struct timeval* timeout = nullptr) {
        return ::select(max_fd_ + 1,
                        &read_set_, &write_set_, &except_set_, timeout);
    }

    //! Do a select() with timeout (in ms)
    int select_timeout(double timeout) {
        if (timeout == std::numeric_limits<double>::infinity())
            return select(nullptr);
        else {
            struct timeval tv;
            tv.tv_sec = static_cast<time_t>(timeout / 1000);
            tv.tv_usec = static_cast<time_t>(
                (timeout / 1000.0 - static_cast<double>(tv.tv_sec)) * 1000000);
            return select(&tv);
        }
    }

protected:
    //! read bit-field
    fd_set read_set_;

    //! write bit-field
    fd_set write_set_;

    //! exception bit-field
    fd_set except_set_;

    //! maximum file descriptor value in bitsets
    int max_fd_ = 0;
};

//! \}

} // namespace tcp
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_TCP_SELECT_HEADER

/******************************************************************************/
