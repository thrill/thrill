/*******************************************************************************
 * thrill/io/wincall_file.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2005-2006 Roman Dementiev <dementiev@ira.uka.de>
 * Copyright (C) 2008-2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/wincall_file.hpp>

#if STXXL_HAVE_WINCALL_FILE

#include <stxxl/bits/common/error_handling.h>
#include <thrill/io/iostats.h>

#ifndef NOMINMAX
  #define NOMINMAX
#endif
#include <windows.h>

namespace thrill {
namespace io {

void wincall_file::serve(void* buffer, offset_type offset, size_type bytes,
                         request::request_type type) {
    scoped_mutex_lock fd_lock(fd_mutex);

    if (bytes > 32 * 1024 * 1024) {
        STXXL_ERRMSG("Using a block size larger than 32 MiB may not work with the " << io_type() << " filetype");
    }

    HANDLE handle = file_des;
    LARGE_INTEGER desired_pos;
    desired_pos.QuadPart = offset;
    if (!SetFilePointerEx(handle, desired_pos, nullptr, FILE_BEGIN))
    {
        STXXL_THROW_WIN_LASTERROR(io_error,
                                  "SetFilePointerEx in wincall_request::serve()" <<
                                  " offset=" << offset <<
                                  " this=" << this <<
                                  " buffer=" << buffer <<
                                  " bytes=" << bytes <<
                                  " type=" << ((type == request::READ) ? "READ" : "WRITE"));
    }
    else
    {
        stats::scoped_read_write_timer read_write_timer(bytes, type == request::WRITE);

        if (type == request::READ)
        {
            DWORD NumberOfBytesRead = 0;
            assert(bytes <= std::numeric_limits<DWORD>::max());
            if (!ReadFile(handle, buffer, (DWORD)bytes, &NumberOfBytesRead, nullptr))
            {
                STXXL_THROW_WIN_LASTERROR(io_error,
                                          "ReadFile" <<
                                          " this=" << this <<
                                          " offset=" << offset <<
                                          " buffer=" << buffer <<
                                          " bytes=" << bytes <<
                                          " type=" << ((type == request::READ) ? "READ" : "WRITE") <<
                                          " NumberOfBytesRead= " << NumberOfBytesRead);
            }
            else if (NumberOfBytesRead != bytes) {
                STXXL_THROW_WIN_LASTERROR(io_error, " partial read: missing " << (bytes - NumberOfBytesRead) << " out of " << bytes << " bytes");
            }
        }
        else
        {
            DWORD NumberOfBytesWritten = 0;
            assert(bytes <= std::numeric_limits<DWORD>::max());
            if (!WriteFile(handle, buffer, (DWORD)bytes, &NumberOfBytesWritten, nullptr))
            {
                STXXL_THROW_WIN_LASTERROR(io_error,
                                          "WriteFile" <<
                                          " this=" << this <<
                                          " offset=" << offset <<
                                          " buffer=" << buffer <<
                                          " bytes=" << bytes <<
                                          " type=" << ((type == request::READ) ? "READ" : "WRITE") <<
                                          " NumberOfBytesWritten= " << NumberOfBytesWritten);
            }
            else if (NumberOfBytesWritten != bytes) {
                STXXL_THROW_WIN_LASTERROR(io_error, " partial write: missing " << (bytes - NumberOfBytesWritten) << " out of " << bytes << " bytes");
            }
        }
    }
}

const char* wincall_file::io_type() const {
    return "wincall";
}

} // namespace io
} // namespace thrill

#endif  // #if STXXL_HAVE_WINCALL_FILE

/******************************************************************************/
