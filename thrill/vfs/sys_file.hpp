/*******************************************************************************
 * thrill/vfs/sys_file.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_VFS_SYS_FILE_HEADER
#define THRILL_VFS_SYS_FILE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/common/zip_stream.hpp>
#include <thrill/vfs/file_io.hpp>

#if defined(_MSC_VER)

#include <io.h>

#else

#include <unistd.h>

#endif

#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace vfs {

/*!
 * Represents a POSIX system file via its file descriptor.
 */
class SysFile : public AbstractFile
{
    static constexpr bool debug = false;

public:
    //! default constructor
    SysFile() : fd_(-1) { }

    //! non-copyable: delete copy-constructor
    SysFile(const SysFile&) = delete;
    //! non-copyable: delete assignment operator
    SysFile& operator = (const SysFile&) = delete;
    //! move-constructor
    SysFile(SysFile&& f) noexcept
        : fd_(f.fd_), pid_(f.pid_) {
        f.fd_ = -1, f.pid_ = 0;
    }
    //! move-assignment
    SysFile& operator = (SysFile&& f) {
        close();
        fd_ = f.fd_, pid_ = f.pid_;
        f.fd_ = -1, f.pid_ = 0;
        return *this;
    }

    //! POSIX write function.
    ssize_t write(const void* data, size_t count) {
        assert(fd_ >= 0);
#if defined(_MSC_VER)
        return ::_write(fd_, data, static_cast<unsigned>(count));
#else
        return ::write(fd_, data, count);
#endif
    }

    //! POSIX read function.
    ssize_t read(void* data, size_t count) {
        assert(fd_ >= 0);
#if defined(_MSC_VER)
        return ::_read(fd_, data, static_cast<unsigned>(count));
#else
        return ::read(fd_, data, count);
#endif
    }

    //! POSIX lseek function from current position.
    ssize_t lseek(off_t offset) {
        assert(fd_ >= 0);
        return ::lseek(fd_, offset, SEEK_CUR);
    }

    //! close the file descriptor
    void close();

    ~SysFile() {
        close();
    }

    //! constructor: use OpenForRead or OpenForWrite.
    explicit SysFile(int fd, int pid = 0) noexcept
        : fd_(fd), pid_(pid) { }

private:
    //! file descriptor
    int fd_ = -1;

#if defined(_MSC_VER)
    using pid_t = int;
#endif

    //! pid of child process to wait for
    pid_t pid_ = 0;
};

/*!
 * Open file for reading and return file descriptor. Handles compressed files by
 * calling a decompressor in a pipe, like "cat $f | gzip -dc |" in bash.
 *
 * \param path Path to open
 */
std::shared_ptr<SysFile> SysOpenReadStream(const std::string& path);

/*!
 * Open file for writing and return file descriptor. Handles compressed files by
 * calling a compressor in a pipe, like "| gzip -d > $f" in bash.
 *
 * \param path Path to open
 */
std::shared_ptr<SysFile> SysOpenWriteStream(const std::string& path);

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_SYS_FILE_HEADER

/******************************************************************************/
