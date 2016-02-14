/*******************************************************************************
 * thrill/io/wfs_file_base.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2005 Roman Dementiev <dementiev@ira.uka.de>
 * Copyright (C) 2008, 2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009, 2010 Johannes Singler <singler@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/error_handling.hpp>
#include <thrill/io/wfs_file_base.hpp>

#if THRILL_WINDOWS

#ifndef NOMINMAX
  #define NOMINMAX
#endif
#include <windows.h>

#include <string>

namespace thrill {
namespace io {

const char* WfsFileBase::io_type() const {
    return "wfs_base";
}

static HANDLE open_file_impl(const std::string& filename, int mode) {
    DWORD dwDesiredAccess = 0;
    DWORD dwShareMode = 0;
    DWORD dwCreationDisposition = 0;
    DWORD dwFlagsAndAttributes = 0;

    if (mode & FileBase::RDONLY)
    {
        dwFlagsAndAttributes |= FILE_ATTRIBUTE_READONLY;
        dwDesiredAccess |= GENERIC_READ;
    }

    if (mode & FileBase::WRONLY)
    {
        dwDesiredAccess |= GENERIC_WRITE;
    }

    if (mode & FileBase::RDWR)
    {
        dwDesiredAccess |= (GENERIC_READ | GENERIC_WRITE);
    }

    if (mode & FileBase::CREAT)
    {
        // ignored
    }

    if (mode & FileBase::TRUNC)
    {
        dwCreationDisposition |= TRUNCATE_EXISTING;
    }
    else
    {
        dwCreationDisposition |= OPEN_ALWAYS;
    }

    if (mode & FileBase::DIRECT)
    {
#if !THRILL_DIRECT_IO_OFF
        dwFlagsAndAttributes |= FILE_FLAG_NO_BUFFERING;
        // TODO(?): try also FILE_FLAG_WRITE_THROUGH option ?
#else
        if (mode & FileBase::REQUIRE_DIRECT) {
            LOG1 << "Error: open()ing " << filename << " with DIRECT mode required, but the system does not support it.";
            return INVALID_HANDLE_VALUE;
        }
        else {
            LOG1 << "Warning: open()ing " << filename << " without DIRECT mode, as the system does not support it.";
        }
#endif
    }

    if (mode & FileBase::SYNC)
    {
        // ignored
    }

    HANDLE file_des = ::CreateFile(filename.c_str(), dwDesiredAccess, dwShareMode, nullptr,
                                   dwCreationDisposition, dwFlagsAndAttributes, nullptr);

    if (file_des != INVALID_HANDLE_VALUE)
        return file_des;

#if !THRILL_DIRECT_IO_OFF
    if ((mode& FileBase::DIRECT) && !(mode & FileBase::REQUIRE_DIRECT))
    {
        LOG1 << "CreateFile() error on path=" << filename << " mode=" << mode << ", retrying without DIRECT mode.";

        dwFlagsAndAttributes &= ~FILE_FLAG_NO_BUFFERING;

        file_des = ::CreateFile(filename.c_str(), dwDesiredAccess, dwShareMode, nullptr,
                                dwCreationDisposition, dwFlagsAndAttributes, nullptr);

        if (file_des != INVALID_HANDLE_VALUE)
            return file_des;
    }
#endif

    THRILL_THROW_WIN_LASTERROR(IoError, "CreateFile() path=" << filename << " mode=" << mode);
}

WfsFileBase::WfsFileBase(
    const std::string& filename,
    int mode) : file_des_(INVALID_HANDLE_VALUE), mode_(mode), filename(filename), locked(false) {
    file_des_ = open_file_impl(filename, mode);

    if (!(mode & NO_LOCK))
    {
        lock();
    }

    if (!(mode_ & RDONLY) && (mode & DIRECT))
    {
        char buf[32768], * part;
        if (!GetFullPathName(filename.c_str(), sizeof(buf), buf, &part))
        {
            LOG1 << "wfs_file_base::wfs_file_base(): GetFullPathName() error for file " << filename;
            bytes_per_sector = 512;
        }
        else
        {
            part[0] = char();
            DWORD bytes_per_sector_;
            if (!GetDiskFreeSpace(buf, nullptr, &bytes_per_sector_, nullptr, nullptr))
            {
                LOG1 << "wfs_file_base::wfs_file_base(): GetDiskFreeSpace() error for path " << buf;
                bytes_per_sector = 512;
            }
            else
                bytes_per_sector = bytes_per_sector_;
        }
    }
}

WfsFileBase::~WfsFileBase() {
    close();
}

void WfsFileBase::close() {
    std::unique_lock<std::mutex> fd_lock(fd_mutex_);

    if (file_des_ == INVALID_HANDLE_VALUE)
        return;

    if (!CloseHandle(file_des_))
        THRILL_THROW_WIN_LASTERROR(IoError, "CloseHandle() of file fd=" << file_des_);

    file_des_ = INVALID_HANDLE_VALUE;
}

void WfsFileBase::lock() {
    std::unique_lock<std::mutex> fd_lock(fd_mutex_);
    if (locked)
        return;  // already locked
    if (LockFile(file_des_, 0, 0, 0xffffffff, 0xffffffff) == 0)
        THRILL_THROW_WIN_LASTERROR(IoError, "LockFile() fd=" << file_des_);
    locked = true;
}

FileBase::offset_type WfsFileBase::_size() {
    LARGE_INTEGER result;
    if (!GetFileSizeEx(file_des_, &result))
        THRILL_THROW_WIN_LASTERROR(IoError, "GetFileSizeEx() fd=" << file_des_);

    return result.QuadPart;
}

FileBase::offset_type WfsFileBase::size() {
    std::unique_lock<std::mutex> fd_lock(fd_mutex_);
    return _size();
}

void WfsFileBase::set_size(offset_type newsize) {
    std::unique_lock<std::mutex> fd_lock(fd_mutex_);
    offset_type cur_size = _size();

    if (!(mode_ & RDONLY))
    {
        LARGE_INTEGER desired_pos;
        desired_pos.QuadPart = newsize;

        bool direct_with_bad_size = (mode_& FileBase::DIRECT) && (newsize % bytes_per_sector);
        if (direct_with_bad_size)
        {
            if (!CloseHandle(file_des_))
                THRILL_THROW_WIN_LASTERROR(IoError, "closing file (call of ::CloseHandle() from set_size) ");

            file_des_ = INVALID_HANDLE_VALUE;
            file_des_ = open_file_impl(filename, WRONLY);
        }

        if (!SetFilePointerEx(file_des_, desired_pos, nullptr, FILE_BEGIN))
            THRILL_THROW_WIN_LASTERROR(IoError,
                                       "SetFilePointerEx() in wfs_file_base::set_size(..) oldsize=" << cur_size <<
                                       " newsize=" << newsize << " ");

        if (!SetEndOfFile(file_des_))
            THRILL_THROW_WIN_LASTERROR(IoError, "SetEndOfFile() oldsize=" << cur_size <<
                                       " newsize=" << newsize << " ");

        if (direct_with_bad_size)
        {
            if (!CloseHandle(file_des_))
                THRILL_THROW_WIN_LASTERROR(IoError, "closing file (call of ::CloseHandle() from set_size) ");

            file_des_ = INVALID_HANDLE_VALUE;
            file_des_ = open_file_impl(filename, mode_ & ~TRUNC);
        }
    }
}

void WfsFileBase::close_remove() {
    close();
    ::DeleteFile(filename.c_str());
}

} // namespace io
} // namespace thrill

#endif // THRILL_WINDOWS

/******************************************************************************/
