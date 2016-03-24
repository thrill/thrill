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

#if THRILL_HAVE_WINCALL_FILE

#include <thrill/io/error_handling.hpp>
#include <thrill/io/iostats.hpp>

#ifndef NOMINMAX
  #define NOMINMAX
#endif
#include <windows.h>

#include <limits>

namespace thrill {
namespace io {

void WincallFile::serve(void* buffer, offset_type offset, size_type bytes,
                        Request::ReadOrWriteType type) {
    std::unique_lock<std::mutex> fd_lock(fd_mutex_);

    if (bytes > 32 * 1024 * 1024) {
        LOG1 << "Using a block size larger than 32 MiB may not work with the " << io_type() << " filetype";
    }

    HANDLE handle = file_des_;
    LARGE_INTEGER desired_pos;
    desired_pos.QuadPart = offset;
    if (!SetFilePointerEx(handle, desired_pos, nullptr, FILE_BEGIN))
    {
        THRILL_THROW_WIN_LASTERROR(
            IoError,
            "SetFilePointerEx in wincall_request::serve()" <<
            " offset=" << offset <<
            " this=" << this <<
            " buffer=" << buffer <<
            " bytes=" << bytes <<
            " type=" << ((type == Request::READ) ? "READ" : "WRITE"));
    }
    else
    {
        Stats::ScopedReadWriteTimer read_write_timer(bytes, type == Request::WRITE);

        if (type == Request::READ)
        {
            DWORD NumberOfBytesRead = 0;
            assert(bytes <= std::numeric_limits<DWORD>::max());
            if (!ReadFile(handle, buffer, (DWORD)bytes, &NumberOfBytesRead, nullptr))
            {
                THRILL_THROW_WIN_LASTERROR(
                    IoError,
                    "ReadFile" <<
                    " this=" << this <<
                    " offset=" << offset <<
                    " buffer=" << buffer <<
                    " bytes=" << bytes <<
                    " type=" << ((type == Request::READ) ? "READ" : "WRITE") <<
                    " NumberOfBytesRead= " << NumberOfBytesRead);
            }
            else if (NumberOfBytesRead != bytes) {
                THRILL_THROW_WIN_LASTERROR(
                    IoError, " partial read: missing " <<
                    (bytes - NumberOfBytesRead) << " out of " <<
                    bytes << " bytes");
            }
        }
        else
        {
            DWORD NumberOfBytesWritten = 0;
            assert(bytes <= std::numeric_limits<DWORD>::max());
            if (!WriteFile(handle, buffer, (DWORD)bytes, &NumberOfBytesWritten, nullptr))
            {
                THRILL_THROW_WIN_LASTERROR(
                    IoError,
                    "WriteFile" <<
                    " this=" << this <<
                    " offset=" << offset <<
                    " buffer=" << buffer <<
                    " bytes=" << bytes <<
                    " type=" << ((type == Request::READ) ? "READ" : "WRITE") <<
                    " NumberOfBytesWritten= " << NumberOfBytesWritten);
            }
            else if (NumberOfBytesWritten != bytes) {
                THRILL_THROW_WIN_LASTERROR(
                    IoError, " partial write: missing " <<
                    (bytes - NumberOfBytesWritten) << " out of " <<
                    bytes << " bytes");
            }
        }
    }
}

const char* WincallFile::io_type() const {
    return "wincall";
}

} // namespace io
} // namespace thrill

#endif  // #if THRILL_HAVE_WINCALL_FILE

/******************************************************************************/
