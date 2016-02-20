/*******************************************************************************
 * thrill/io/syscall_file.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2010 Johannes Singler <singler@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/config.hpp>
#include <thrill/io/error_handling.hpp>
#include <thrill/io/iostats.hpp>
#include <thrill/io/request.hpp>
#include <thrill/io/syscall_file.hpp>
#include <thrill/io/ufs_platform.hpp>

#include <limits>

namespace thrill {
namespace io {

void SyscallFile::serve(void* buffer, offset_type offset, size_type bytes,
                        Request::ReadOrWriteType type) {
    std::unique_lock<std::mutex> fd_lock(fd_mutex_);

    char* cbuffer = static_cast<char*>(buffer);

    Stats::scoped_read_write_timer read_write_timer(bytes, type == Request::WRITE);

    while (bytes > 0)
    {
        off_t rc = ::lseek(file_des_, offset, SEEK_SET);
        if (rc < 0)
        {
            THRILL_THROW_ERRNO(IoError,
                               " this=" << this <<
                               " call=::lseek(fd,offset,SEEK_SET)" <<
                               " path=" << path_ <<
                               " fd=" << file_des_ <<
                               " offset=" << offset <<
                               " buffer=" << cbuffer <<
                               " bytes=" << bytes <<
                               " type=" << ((type == Request::READ) ? "READ" : "WRITE") <<
                               " rc=" << rc);
        }

        if (type == Request::READ)
        {
#if THRILL_MSVC
            assert(bytes <= std::numeric_limits<unsigned int>::max());
            if ((rc = ::read(file_des_, cbuffer, (unsigned int)bytes)) <= 0)
#else
            if ((rc = ::read(file_des_, cbuffer, bytes)) <= 0)
#endif
            {
                THRILL_THROW_ERRNO
                    (IoError,
                    " this=" << this <<
                    " call=::read(fd,buffer,bytes)" <<
                    " path=" << path_ <<
                    " fd=" << file_des_ <<
                    " offset=" << offset <<
                    " buffer=" << buffer <<
                    " bytes=" << bytes <<
                    " type=" << "READ" <<
                    " rc=" << rc);
            }
            bytes = (size_type)(bytes - rc);
            offset += rc;
            cbuffer += rc;

            if (bytes > 0 && offset == this->_size())
            {
                // read request extends past end-of-file
                // fill reminder with zeroes
                memset(cbuffer, 0, bytes);
                bytes = 0;
            }
        }
        else
        {
#if THRILL_MSVC
            assert(bytes <= std::numeric_limits<unsigned int>::max());
            if ((rc = ::write(file_des_, cbuffer, (unsigned int)bytes)) <= 0)
#else
            if ((rc = ::write(file_des_, cbuffer, bytes)) <= 0)
#endif
            {
                THRILL_THROW_ERRNO
                    (IoError,
                    " this=" << this <<
                    " call=::write(fd,buffer,bytes)" <<
                    " path=" << path_ <<
                    " fd=" << file_des_ <<
                    " offset=" << offset <<
                    " buffer=" << buffer <<
                    " bytes=" << bytes <<
                    " type=" << "WRITE" <<
                    " rc=" << rc);
            }
            bytes = (size_type)(bytes - rc);
            offset += rc;
            cbuffer += rc;
        }
    }
}

const char* SyscallFile::io_type() const {
    return "syscall";
}

} // namespace io
} // namespace thrill

/******************************************************************************/
