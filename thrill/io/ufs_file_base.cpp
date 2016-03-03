/*******************************************************************************
 * thrill/io/ufs_file_base.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002, 2005, 2008 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008 Ilja Andronov <sni4ok@yandex.ru>
 * Copyright (C) 2008-2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/config.hpp>
#include <thrill/io/error_handling.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/io/ufs_file_base.hpp>
#include <thrill/io/ufs_platform.hpp>

#include <string>

namespace thrill {
namespace io {

const char* UfsFileBase::io_type() const {
    return "ufs_base";
}

UfsFileBase::UfsFileBase(
    const std::string& filename,
    int mode)
    : file_des_(-1), mode_(mode), path_(filename) {
    int flags = 0;

    if (mode & RDONLY)
    {
        flags |= O_RDONLY;
    }

    if (mode & WRONLY)
    {
        flags |= O_WRONLY;
    }

    if (mode & RDWR)
    {
        flags |= O_RDWR;
    }

    if (mode & CREAT)
    {
        flags |= O_CREAT;
    }

    if (mode & TRUNC)
    {
        flags |= O_TRUNC;
    }

    if ((mode & DIRECT) || (mode & REQUIRE_DIRECT))
    {
#if __APPLE__
        // no additional open flags are required for Mac OS X
#else
        flags |= O_DIRECT;
#endif
    }

    if (mode & SYNC)
    {
        flags |= O_RSYNC;
        flags |= O_DSYNC;
        flags |= O_SYNC;
    }

#if THRILL_WINDOWS
    flags |= O_BINARY;                     // the default in MS is TEXT mode
#endif

#if THRILL_WINDOWS || defined(__MINGW32__)
    const int perms = S_IREAD | S_IWRITE;
#else
    const int perms = S_IREAD | S_IWRITE | S_IRGRP | S_IWGRP;
#endif

    if ((file_des_ = ::open(filename.c_str(), flags, perms)) >= 0)
    {
        need_alignment_ = (mode & DIRECT);
        _after_open();
        return;
    }

    if ((mode & DIRECT) && !(mode & REQUIRE_DIRECT) && errno == EINVAL)
    {
        LOG1 << "open() error on path=" << filename
             << " flags=" << flags << ", retrying without O_DIRECT.";

        flags &= ~O_DIRECT;
        mode_ &= ~DIRECT;

        if ((file_des_ = ::open(filename.c_str(), flags, perms)) >= 0)
        {
            _after_open();
            return;
        }
    }

    THRILL_THROW_ERRNO(IoError, "open() rc=" << file_des_ << " path=" << filename << " flags=" << flags);
}

UfsFileBase::~UfsFileBase() {
    close();
}

void UfsFileBase::_after_open() {
    // stat file type
#if THRILL_WINDOWS || defined(__MINGW32__)
    struct _stat64 st;
    THRILL_THROW_ERRNO_NE_0(::_fstat64(file_des_, &st), IoError,
                            "_fstat64() path=" << path_ << " fd=" << file_des_);
#else
    struct stat st;
    THRILL_THROW_ERRNO_NE_0(::fstat(file_des_, &st), IoError,
                            "fstat() path=" << path_ << " fd=" << file_des_);
#endif
    is_device_ = S_ISBLK(st.st_mode) ? true : false;

#if __APPLE__
    if (mode_ & REQUIRE_DIRECT) {
        THRILL_THROW_ERRNO_NE_0(fcntl(file_des_, F_NOCACHE, 1), IoError,
                                "fcntl() path=" << path_ << " fd=" << file_des_);
        THRILL_THROW_ERRNO_NE_0(fcntl(file_des_, F_RDAHEAD, 0), IoError,
                                "fcntl() path=" << path_ << " fd=" << file_des_);
    }
    else if (mode_ & DIRECT) {
        if (fcntl(file_des_, F_NOCACHE, 1) != 0) {
            LOG1 << "fcntl(fd,F_NOCACHE,1) failed on path=" << path_
                 << " fd=" << file_des_ << " : " << strerror(errno);
        }
        if (fcntl(file_des_, F_RDAHEAD, 0) != 0) {
            LOG1 << "fcntl(fd,F_RDAHEAD,0) failed on path=" << path_
                 << " fd=" << file_des_ << " : " << strerror(errno);
        }
    }
#endif

    // successfully opened file descriptor
    if (!(mode_ & NO_LOCK))
        lock();
}

void UfsFileBase::close() {
    std::unique_lock<std::mutex> fd_lock(fd_mutex_);

    if (file_des_ == -1)
        return;

    if (::close(file_des_) < 0)
        THRILL_THROW_ERRNO(IoError, "close() fd=" << file_des_);

    file_des_ = -1;
}

void UfsFileBase::lock() {
#if THRILL_WINDOWS || defined(__MINGW32__)
    // not yet implemented
#else
    std::unique_lock<std::mutex> fd_lock(fd_mutex_);
    struct flock lock_struct;
    lock_struct.l_type = (short)(mode_ & RDONLY ? F_RDLCK : F_RDLCK | F_WRLCK);
    lock_struct.l_whence = SEEK_SET;
    lock_struct.l_start = 0;
    lock_struct.l_len = 0; // lock all bytes
    if ((::fcntl(file_des_, F_SETLK, &lock_struct)) < 0)
        THRILL_THROW_ERRNO(IoError, "fcntl(,F_SETLK,) path=" << path_ << " fd=" << file_des_);
#endif
}

FileBase::offset_type UfsFileBase::_size() {
    // We use lseek SEEK_END to find the file size. This works for raw devices
    // (where stat() returns zero), and we need not reset the position because
    // serve() always lseek()s before read/write.

    off_t rc = ::lseek(file_des_, 0, SEEK_END);
    if (rc < 0)
        THRILL_THROW_ERRNO(IoError, "lseek(fd,0,SEEK_END) path=" << path_ << " fd=" << file_des_);

    // return value is already the total size
    return rc;
}

FileBase::offset_type UfsFileBase::size() {
    std::unique_lock<std::mutex> fd_lock(fd_mutex_);
    return _size();
}

void UfsFileBase::set_size(offset_type newsize) {
    std::unique_lock<std::mutex> fd_lock(fd_mutex_);
    return _set_size(newsize);
}

void UfsFileBase::_set_size(offset_type newsize) {
    offset_type cur_size = _size();

    if (!(mode_ & RDONLY) && !is_device_)
    {
#if THRILL_WINDOWS || defined(__MINGW32__)
        HANDLE hfile = (HANDLE)::_get_osfhandle(file_des_);
        THRILL_THROW_ERRNO_NE_0((hfile == INVALID_HANDLE_VALUE), IoError,
                                "_get_osfhandle() path=" << path_ << " fd=" << file_des_);

        LARGE_INTEGER desired_pos;
        desired_pos.QuadPart = newsize;

        if (!SetFilePointerEx(hfile, desired_pos, nullptr, FILE_BEGIN))
            THRILL_THROW_WIN_LASTERROR(IoError,
                                       "SetFilePointerEx in ufs_file_base::set_size(..) oldsize=" << cur_size <<
                                       " newsize=" << newsize << " ");

        if (!SetEndOfFile(hfile))
            THRILL_THROW_WIN_LASTERROR(IoError,
                                       "SetEndOfFile oldsize=" << cur_size <<
                                       " newsize=" << newsize << " ");
#else
        THRILL_THROW_ERRNO_NE_0(::ftruncate(file_des_, newsize), IoError,
                                "ftruncate() path=" << path_ << " fd=" << file_des_);
#endif
    }

#if !THRILL_WINDOWS
    if (newsize > cur_size)
        THRILL_THROW_ERRNO_LT_0(::lseek(file_des_, newsize - 1, SEEK_SET), IoError,
                                "lseek() path=" << path_ << " fd=" << file_des_ << " pos=" << newsize - 1);
#endif
}

void UfsFileBase::close_remove() {
    close();

    if (is_device_) {
        LOG1 << "remove() path=" << path_ << " skipped as file is device node";
        return;
    }

    if (::remove(path_.c_str()) != 0)
        LOG1 << "remove() error on path=" << path_ << " error=" << strerror(errno);
}

void UfsFileBase::unlink() {
    if (is_device_) {
        LOG1 << "unlink() path=" << path_ << " skipped as file is device node";
        return;
    }

    if (::unlink(path_.c_str()) != 0)
        THRILL_THROW_ERRNO(IoError, "unlink() path=" << path_ << " fd=" << file_des_);
}

bool UfsFileBase::is_device() const {
    return is_device_;
}

} // namespace io
} // namespace thrill

/******************************************************************************/
