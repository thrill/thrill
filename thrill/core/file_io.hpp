/*******************************************************************************
 * thrill/core/file_io.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_FILE_IO_HEADER
#define THRILL_CORE_FILE_IO_HEADER

#include <glob.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>

namespace thrill {
namespace core {

class FileIO
{
public:
    using FileSizePair = std::pair<std::string, size_t>;

    // Returns true, if file at filepath is compressed (e.g, ends with
    // '.[gz/bz2,xz,lzo]')
    bool IsCompressed(const std::string& path) {
        return common::ends_with(path, ".gz") ||
               common::ends_with(path, ".bz2") ||
               common::ends_with(path, ".xz") ||
               common::ends_with(path, ".lzo") ||
               common::ends_with(path, ".lz4");
    }

    /*!
     * Adds a pair of filename and size prefixsum (in bytes) for all files in
     * the input path.
     *
     * \param path Input path
     */
    std::pair<std::vector<FileSizePair>, bool> ReadFileList(const std::string& path) {

        static const bool debug = false;

        bool contains_compressed_file = false;

        std::vector<FileSizePair> filesize_prefix;
        glob_t glob_result;
        struct stat filestat;
        glob(path.c_str(), GLOB_TILDE, nullptr, &glob_result);
        size_t directory_size = 0;

        for (unsigned int i = 0; i < glob_result.gl_pathc; ++i) {
            const char* filepath = glob_result.gl_pathv[i];

            if (stat(filepath, &filestat)) {
                throw std::runtime_error(
                          "ERROR: Invalid file " + std::string(filepath));
            }
            if (!S_ISREG(filestat.st_mode)) continue;

            if (IsCompressed(filepath)) {
                contains_compressed_file = true;
            }

            LOG << "Added file " << filepath << ", new total size "
                << directory_size;

            filesize_prefix.emplace_back(std::move(filepath), directory_size);
            directory_size += filestat.st_size;
        }
        filesize_prefix.emplace_back("", directory_size);
        globfree(&glob_result);

        return std::make_pair(filesize_prefix, contains_compressed_file);
    }
};

/*!
 * Open file for reading and return file descriptor. Handles compressed files by
 * calling a decompressor in a pipe, like "cat $f | gzip -dc |" in bash.
 *
 * \param path Path to open
 */
int OpenFileForRead(const std::string& path) {

    // first open the file and see if it exists at all.

    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw common::SystemException("Cannot open file " + path, errno);
    }

    // then figure out whether we need to pipe it through a decompressor.

    const char* decompressor;

    if (common::ends_with(path, ".gz")) {
        decompressor = "gzip";
    }
    else if (common::ends_with(path, ".bz2")) {
        decompressor = "bzip2";
    }
    else if (common::ends_with(path, ".xz")) {
        decompressor = "xz";
    }
    else if (common::ends_with(path, ".lzo")) {
        decompressor = "lzop";
    }
    else if (common::ends_with(path, ".lz4")) {
        decompressor = "lz4";
    }
    else {
        // not a compressed file
        return fd;
    }

    // if decompressor: fork a child program which calls the decompressor and
    // connect file descriptors via a pipe.

    // pipe[0] = read, pipe[1] = write
    int pipefd[2];
    if (pipe(pipefd) != 0)
        throw common::SystemException("Error creating pipe", errno);

    pid_t pid = fork();
    if (pid == 0) {
        // close read end
        close(pipefd[0]);

        // replace stdin with file descriptor to file opened above.
        dup2(fd, STDIN_FILENO);
        // replace stdout with pipe going back to Thrill process
        dup2(pipefd[1], STDOUT_FILENO);

        execlp(decompressor, decompressor, "-df", nullptr);

        LOG1 << "Pipe execution failed: " << strerror(errno);
        // close write end
        close(pipefd[1]);
        exit(-1);
    }

    // TODO(tb): we have to reap the child process! currently it is a zombie.

    // close pipe write end
    close(pipefd[1]);

    // close the file descriptor
    close(fd);

    return pipefd[0];
}

/*!
 * Represents a POSIX system file via its file descriptor.
 */
class SysFile
{
public:

    //! default constructor
    SysFile() : fd_(-1) { }

    static SysFile OpenForRead(const std::string& path) {
        return SysFile(OpenFileForRead(path));
    }

    static SysFile OpenForWrite(const std::string& path);

    //! non-copyable: delete copy-constructor
    SysFile(const SysFile &) = delete;
    //! non-copyable: delete assignment operator
    SysFile & operator = (const SysFile &) = delete;
    //! move-constructor
    SysFile(SysFile && f)
        : fd_(f.fd_), pid_(f.pid_) {
        f.fd_ = -1;
        f.pid_ = 0;
    }

    //! POSIX write function.
    ssize_t write(const void* data, size_t count) {
        return ::write(fd_, data, count);
    }

    //! close the file descriptor
    void close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        if (pid_ != 0) {
            int status;
            pid_t p = waitpid(pid_, &status, 0);
            if (p != pid_) {
                throw common::SystemException(
                    "SysFile: waitpid() failed to return child", errno);
            }
            if (WIFEXITED(status)) {
                // child program exited normally
                if (WEXITSTATUS(status) != 0) {
                    throw common::SystemException(
                        "SysFile: child failed to return code "
                        + std::to_string(WEXITSTATUS(status)));
                }
                else {
                    // zero return code. good.
                }
            }
            else if (WIFSIGNALED(status)) {
                throw common::SystemException(
                        "SysFile: child killed by signal "
                        + std::to_string(WTERMSIG(status)));
            }
            else {
                throw common::SystemException(
                    "SysFile: child failed with an unknown error", errno);
            }
            pid_ = 0;
        }
    }

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

/*!
 * Open file for writing and return file descriptor. Handles compressed files by
 * calling a compressor in a pipe, like "| gzip -d > $f" in bash.
 *
 * \param path Path to open
 */
SysFile SysFile::OpenForWrite(const std::string& path) {

    // first create the file and see if we can write it at all.

    int fd = open(path.c_str(), O_CREAT | O_WRONLY, 0666);
    if (fd < 0) {
        throw common::SystemException("Cannot create file " + path, errno);
    }

    // then figure out whether we need to pipe it through a compressor.

    const char* compressor;

    if (common::ends_with(path, ".gz")) {
        compressor = "gzip";
    }
    else if (common::ends_with(path, ".bz2")) {
        compressor = "bzip2";
    }
    else if (common::ends_with(path, ".xz")) {
        compressor = "xz";
    }
    else if (common::ends_with(path, ".lzo")) {
        compressor = "lzop";
    }
    else if (common::ends_with(path, ".lz4")) {
        compressor = "lz4";
    }
    else {
        // not a compressed file
        return SysFile(fd);
    }

    // if compressor: fork a child program which calls the compressor and
    // connect file descriptors via a pipe.

    // pipe[0] = read, pipe[1] = write
    int pipefd[2];
    if (pipe(pipefd) != 0)
        throw common::SystemException("Error creating pipe", errno);

    if (fcntl(pipefd[0], F_SETFD, FD_CLOEXEC) != 0) {
        LOG1 << "Error setting FD_CLOEXEC on child pipe: " << strerror(errno);
    }
    if (fcntl(pipefd[1], F_SETFD, FD_CLOEXEC) != 0) {
        LOG1 << "Error setting FD_CLOEXEC on child pipe: " << strerror(errno);
    }

    pid_t pid = fork();
    if (pid == 0) {
        // close write end
        ::close(pipefd[1]);

        // replace stdin with pipe
        dup2(pipefd[0], STDIN_FILENO);
        ::close(pipefd[0]);
        // replace stdout with file descriptor to file created above.
        dup2(fd, STDOUT_FILENO);
        ::close(fd);

        execlp(compressor, compressor, nullptr);

        LOG1 << "Pipe execution failed: " << strerror(errno);
        // close write end
        ::close(pipefd[1]);
        exit(-1);
    }
    else if (pid < 0) {
        throw common::SystemException("Error creating child process", errno);
    }

    // close read end
    ::close(pipefd[0]);
    // close file descriptor (it is used by the fork)
    ::close(fd);

    return SysFile(pipefd[1], pid);
}

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_FILE_IO_HEADER

/******************************************************************************/
