/*******************************************************************************
 * thrill/core/file_io.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/common/porting.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/core/file_io.hpp>

#include <fcntl.h>
#include <glob.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <string>
#include <vector>

namespace thrill {
namespace core {

//! function which takes pathbase and replaces $$$ with worker and ### with
//! the file_part values.
std::string make_path(const std::string& pathbase,
                      size_t worker, size_t file_part) {

    static const bool debug = false;

    using size_type = std::string::size_type;

    std::string out_path = pathbase;
    {
        // replace dollar
        size_type dollar_end = out_path.rfind('$');
        size_type dollar_begin = out_path.find_last_not_of('$', dollar_end);

        size_type dollar_length = dollar_end - dollar_begin;
        if (dollar_length == 0) dollar_length = 4;

        sLOG << "dollar_length" << dollar_length;
        out_path.replace(dollar_begin + 1, dollar_length,
                         common::str_snprintf<>(dollar_length + 2, "%0*lu",
                                                static_cast<int>(dollar_length),
                                                worker));
    }
    {
        // replace hash signs
        size_type hash_end = out_path.rfind('#');
        size_type hash_begin = out_path.find_last_not_of('#', hash_end);

        size_type hash_length = hash_end - hash_begin;
        if (hash_length == 0) hash_length = 10;

        sLOG << "hash_length" << hash_length;
        out_path.replace(hash_begin + 1, hash_length,
                         common::str_snprintf<>(hash_length + 2, "%0*lu",
                                                static_cast<int>(hash_length),
                                                file_part));
    }
    return out_path;
}

std::vector<std::string> GlobFilePattern(const std::string& path) {

    std::vector<std::string> files;
    glob_t glob_result;
    glob(path.c_str(), GLOB_TILDE, nullptr, &glob_result);

    for (unsigned int i = 0; i < glob_result.gl_pathc; ++i) {
        files.push_back(glob_result.gl_pathv[i]);
    }
    globfree(&glob_result);

    return files;
}

std::vector<FileSizePair> GlobFileSizePrefixSum(const std::string& path) {

    std::vector<FileSizePair> file_size_pairs;
    struct stat filestat;
    size_t directory_size = 0;
    std::vector<std::string> files = GlobFilePattern(path);

    for (const std::string& file : files) {

        if (stat(file.c_str(), &filestat)) {
            throw std::runtime_error(
                      "ERROR: Invalid file " + std::string(file));
        }
        if (!S_ISREG(filestat.st_mode)) continue;

        file_size_pairs.emplace_back(std::move(file), directory_size);
        directory_size += filestat.st_size;
    }
    file_size_pairs.emplace_back("", directory_size);

    return file_size_pairs;
}

/******************************************************************************/

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
}

SysFile SysFile::OpenForRead(const std::string& path) {

    // first open the file and see if it exists at all.

    int fd = open(path.c_str(), O_RDONLY, 0);
    if (fd < 0) {
        throw common::ErrnoException("Cannot open file " + path);
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
        if (fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) {
            throw common::ErrnoException("Error setting FD_CLOEXEC on SysFile");
        }

        sLOG << "SysFile::OpenForRead(): filefd" << fd;

        return SysFile(fd);
    }

    // if decompressor: fork a child program which calls the decompressor and
    // connect file descriptors via a pipe.

    // pipe[0] = read, pipe[1] = write
    int pipefd[2];
    common::make_pipe(pipefd);

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

    return SysFile(pipefd[0], pid);
}

SysFile SysFile::OpenForWrite(const std::string& path) {

    // first create the file and see if we can write it at all.

    int fd = open(path.c_str(), O_CREAT | O_WRONLY, 0666);
    if (fd < 0) {
        throw common::ErrnoException("Cannot create file " + path);
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
        if (fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) {
            throw common::ErrnoException("Error setting FD_CLOEXEC on SysFile");
        }

        sLOG << "SysFile::OpenForWrite(): filefd" << fd;

        return SysFile(fd);
    }

    // if compressor: fork a child program which calls the compressor and
    // connect file descriptors via a pipe.

    // pipe[0] = read, pipe[1] = write
    int pipefd[2];
    common::make_pipe(pipefd);

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

    return SysFile(pipefd[1], pid);
}

} // namespace core
} // namespace thrill

/******************************************************************************/
