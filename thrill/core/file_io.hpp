/*******************************************************************************
 * thrill/core/file_io.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_FILE_IO_HEADER
#define THRILL_CORE_FILE_IO_HEADER

#include <thrill/common/logger.hpp>
#include <thrill/common/system_exception.hpp>

#include <unistd.h>

#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

std::string make_path(const std::string& pathbase,
                      size_t worker, size_t file_part);

// Returns true, if file at filepath is compressed (e.g, ends with
// '.[gz/bz2,xz,lzo]')
static inline
bool IsCompressed(const std::string& path) {
    return common::ends_with(path, ".gz") ||
           common::ends_with(path, ".bz2") ||
           common::ends_with(path, ".xz") ||
           common::ends_with(path, ".lzo") ||
           common::ends_with(path, ".lz4");
}

using FileSizePair = std::pair<std::string, size_t>;

/*!
 * Adds a pair of filename and size prefixsum (in bytes) for all files in
 * the input path.
 *
 * \param path Input path
 */
std::vector<FileSizePair> GlobFileSizePrefixSum(const std::string& path);

/*!
 * Returns a vector of all files found by glob in the input path.
 *
 * \param path Input path
 */
std::vector<std::string> GlobFilePattern(const std::string& path);

/*!
 * Represents a POSIX system file via its file descriptor.
 */
class SysFile
{
    static const bool debug = false;

public:
    //! default constructor
    SysFile() : fd_(-1) { }

    /*!
     * Open file for reading and return file descriptor. Handles compressed
     * files by calling a decompressor in a pipe, like "cat $f | gzip -dc |" in
     * bash.
     *
     * \param path Path to open
     */
    static SysFile OpenForRead(const std::string& path);

    /*!
     * Open file for writing and return file descriptor. Handles compressed
     * files by calling a compressor in a pipe, like "| gzip -d > $f" in bash.
     *
     * \param path Path to open
     */
    static SysFile OpenForWrite(const std::string& path);

    //! non-copyable: delete copy-constructor
    SysFile(const SysFile&) = delete;
    //! non-copyable: delete assignment operator
    SysFile& operator = (const SysFile&) = delete;
    //! move-constructor
    SysFile(SysFile&& f)
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
        return ::write(fd_, data, count);
    }

    //! POSIX read function.
    ssize_t read(void* data, size_t count) {
        assert(fd_ >= 0);
        return ::read(fd_, data, count);
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

protected:
    //! protected constructor: use OpenForRead or OpenForWrite.
    explicit SysFile(int fd, int pid = 0)
        : fd_(fd), pid_(pid) { }

    //! file descriptor
    int fd_ = -1;

    //! pid of child process to wait for
    pid_t pid_ = 0;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_FILE_IO_HEADER

/******************************************************************************/
