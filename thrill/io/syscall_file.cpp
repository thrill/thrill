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

#include "error_handling.hpp"
#include "ufs_platform.hpp"
#include <thrill/common/config.hpp>
#include <thrill/io/iostats.hpp>
#include <thrill/io/request.hpp>
#include <thrill/io/request_interface.hpp>
#include <thrill/io/syscall_file.hpp>

namespace thrill {
namespace io {

void syscall_file::serve(void* buffer, offset_type offset, size_type bytes,
                         request::request_type type) {
    std::unique_lock<std::mutex> fd_lock(fd_mutex);

    char* cbuffer = static_cast<char*>(buffer);

    stats::scoped_read_write_timer read_write_timer(bytes, type == request::WRITE);

    while (bytes > 0)
    {
        off_t rc = ::lseek(file_des, offset, SEEK_SET);
        if (rc < 0)
        {
            STXXL_THROW_ERRNO(io_error,
                              " this=" << this <<
                              " call=::lseek(fd,offset,SEEK_SET)" <<
                              " path=" << filename <<
                              " fd=" << file_des <<
                              " offset=" << offset <<
                              " buffer=" << cbuffer <<
                              " bytes=" << bytes <<
                              " type=" << ((type == request::READ) ? "READ" : "WRITE") <<
                              " rc=" << rc);
        }

        if (type == request::READ)
        {
#if STXXL_MSVC
            assert(bytes <= std::numeric_limits<unsigned int>::max());
            if ((rc = ::read(file_des, cbuffer, (unsigned int)bytes)) <= 0)
#else
            if ((rc = ::read(file_des, cbuffer, bytes)) <= 0)
#endif
            {
                STXXL_THROW_ERRNO
                    (io_error,
                    " this=" << this <<
                    " call=::read(fd,buffer,bytes)" <<
                    " path=" << filename <<
                    " fd=" << file_des <<
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
#if STXXL_MSVC
            assert(bytes <= std::numeric_limits<unsigned int>::max());
            if ((rc = ::write(file_des, cbuffer, (unsigned int)bytes)) <= 0)
#else
            if ((rc = ::write(file_des, cbuffer, bytes)) <= 0)
#endif
            {
                STXXL_THROW_ERRNO
                    (io_error,
                    " this=" << this <<
                    " call=::write(fd,buffer,bytes)" <<
                    " path=" << filename <<
                    " fd=" << file_des <<
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

const char* syscall_file::io_type() const {
    return "syscall";
}

} // namespace io
} // namespace thrill

/******************************************************************************/
