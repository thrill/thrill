/*******************************************************************************
 * thrill/vfs/sys_file.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/sys_file.hpp>

#include <thrill/api/context.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>

#include <fcntl.h>
#include <sys/stat.h>

#if !defined(_MSC_VER)

#include <dirent.h>
#include <glob.h>
#include <sys/wait.h>
#include <unistd.h>

#if !defined(O_BINARY)
#define O_BINARY 0
#endif

#else

#include <io.h>
#include <windows.h>

#define S_ISREG(m)       (((m) & _S_IFMT) == _S_IFREG)

#endif

#include <algorithm>
#include <string>
#include <vector>

namespace thrill {
namespace vfs {

/*!
 * Represents a POSIX system file via its file descriptor.
 */
class SysFile final : public AbstractFile
{
    static constexpr bool debug = false;

public:
    //! default constructor
    SysFile() : fd_(-1) { }

    //! constructor: use OpenForRead or OpenForWrite.
    explicit SysFile(int fd, int pid = 0) noexcept
        : fd_(fd), pid_(pid) { }

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

    ~SysFile() {
        close();
    }

    //! POSIX write function.
    ssize_t write(const void* data, size_t count) final {
        assert(fd_ >= 0);
#if defined(_MSC_VER)
        return ::_write(fd_, data, static_cast<unsigned>(count));
#else
        return ::write(fd_, data, count);
#endif
    }

    //! POSIX read function.
    ssize_t read(void* data, size_t count) final {
        assert(fd_ >= 0);
#if defined(_MSC_VER)
        return ::_read(fd_, data, static_cast<unsigned>(count));
#else
        return ::read(fd_, data, count);
#endif
    }

    //! POSIX lseek function from current position.
    ssize_t lseek(off_t offset) final {
        assert(fd_ >= 0);
        return ::lseek(fd_, offset, SEEK_CUR);
    }

    //! close the file descriptor
    void close() final;

private:
    //! file descriptor
    int fd_ = -1;

#if defined(_MSC_VER)
    using pid_t = int;
#endif

    //! pid of child process to wait for
    pid_t pid_ = 0;
};

void SysFile::close() {
    if (fd_ >= 0) {
        sLOG << "SysFile::close(): fd" << fd_;
        if (::close(fd_) != 0)
        {
            LOG1 << "SysFile::close()"
                 << " fd_=" << fd_
                 << " errno=" << errno
                 << " error=" << strerror(errno);
        }
        fd_ = -1;
    }
#if !defined(_MSC_VER)
    if (pid_ != 0) {
        sLOG << "SysFile::close(): waitpid for" << pid_;
        int status;
        pid_t p = waitpid(pid_, &status, 0);
        if (p != pid_) {
            throw common::SystemException(
                      "SysFile: waitpid() failed to return child");
        }
        if (WIFEXITED(status)) {
            // child program exited normally
            if (WEXITSTATUS(status) != 0) {
                throw common::ErrnoException(
                          "SysFile: child failed with return code "
                          + std::to_string(WEXITSTATUS(status)));
            }
            else {
                // zero return code. good.
            }
        }
        else if (WIFSIGNALED(status)) {
            throw common::ErrnoException(
                      "SysFile: child killed by signal "
                      + std::to_string(WTERMSIG(status)));
        }
        else {
            throw common::ErrnoException(
                      "SysFile: child failed with an unknown error");
        }
        pid_ = 0;
    }
#endif
}

/******************************************************************************/

std::shared_ptr<AbstractFile> SysOpenReadStream(const std::string& path) {

    static constexpr bool debug = false;

    // first open the file and see if it exists at all.

    int fd = ::open(path.c_str(), O_RDONLY | O_BINARY, 0);
    if (fd < 0) {
        throw common::ErrnoException("Cannot open file " + path);
    }

    // then figure out whether we need to pipe it through a decompressor.

    const char* decompressor;

    if (common::EndsWith(path, ".gz")) {
        decompressor = "gzip";
    }
    else if (common::EndsWith(path, ".bz2")) {
        decompressor = "bzip2";
    }
    else if (common::EndsWith(path, ".xz")) {
        decompressor = "xz";
    }
    else if (common::EndsWith(path, ".lzo")) {
        decompressor = "lzop";
    }
    else if (common::EndsWith(path, ".lz4")) {
        decompressor = "lz4";
    }
    else {
        // not a compressed file
        common::PortSetCloseOnExec(fd);

        sLOG << "SysFile::OpenForRead(): filefd" << fd;

        return std::make_shared<SysFile>(fd);
    }

#if defined(_MSC_VER)
    throw common::SystemException(
              "Reading compressed files is not supported on windows, yet. "
              "Please submit a patch.");
#else
    // if decompressor: fork a child program which calls the decompressor and
    // connect file descriptors via a pipe.

    // pipe[0] = read, pipe[1] = write
    int pipefd[2];
    common::MakePipe(pipefd);

    pid_t pid = fork();
    if (pid == 0) {
        // close read end
        ::close(pipefd[0]);

        // replace stdin with file descriptor to file opened above.
        dup2(fd, STDIN_FILENO);
        ::close(fd);
        // replace stdout with pipe going back to Thrill process
        dup2(pipefd[1], STDOUT_FILENO);
        ::close(pipefd[1]);

        execlp(decompressor, decompressor, "-d", nullptr);

        LOG1 << "Pipe execution failed: " << strerror(errno);
        // close write end
        ::close(pipefd[1]);
        exit(-1);
    }
    else if (pid < 0) {
        throw common::ErrnoException("Error creating child process");
    }

    sLOG << "SysFile::OpenForRead(): pipefd" << pipefd[0] << "to pid" << pid;

    // close pipe write end
    ::close(pipefd[1]);

    // close the file descriptor
    ::close(fd);

    return std::make_shared<SysFile>(pipefd[0], pid);
#endif
}

std::shared_ptr<AbstractFile> SysOpenWriteStream(const std::string& path) {

    static constexpr bool debug = false;

    // first create the file and see if we can write it at all.

    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_BINARY, 0666);
    if (fd < 0) {
        throw common::ErrnoException("Cannot create file " + path);
    }

    // then figure out whether we need to pipe it through a compressor.

    const char* compressor;

    if (common::EndsWith(path, ".gz")) {
        compressor = "gzip";
    }
    else if (common::EndsWith(path, ".bz2")) {
        compressor = "bzip2";
    }
    else if (common::EndsWith(path, ".xz")) {
        compressor = "xz";
    }
    else if (common::EndsWith(path, ".lzo")) {
        compressor = "lzop";
    }
    else if (common::EndsWith(path, ".lz4")) {
        compressor = "lz4";
    }
    else {
        // not a compressed file
        common::PortSetCloseOnExec(fd);

        sLOG << "SysFile::OpenForWrite(): filefd" << fd;

        return std::make_shared<SysFile>(fd);
    }

#if defined(_MSC_VER)
    throw common::SystemException(
              "Reading compressed files is not supported on windows, yet. "
              "Please submit a patch.");
#else
    // if compressor: fork a child program which calls the compressor and
    // connect file descriptors via a pipe.

    // pipe[0] = read, pipe[1] = write
    int pipefd[2];
    common::MakePipe(pipefd);

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
        // close read end
        ::close(pipefd[0]);
        exit(-1);
    }
    else if (pid < 0) {
        throw common::ErrnoException("Error creating child process");
    }

    sLOG << "SysFile::OpenForWrite(): pipefd" << pipefd[0] << "to pid" << pid;

    // close read end
    ::close(pipefd[0]);

    // close file descriptor (it is used by the fork)
    ::close(fd);

    return std::make_shared<SysFile>(pipefd[1], pid);
#endif
}

} // namespace vfs
} // namespace thrill

/******************************************************************************/
