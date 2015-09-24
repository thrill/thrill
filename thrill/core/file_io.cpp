/*******************************************************************************
 * thrill/core/file_io.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/porting.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/core/file_io.hpp>
#include <thrill/core/simple_glob.hpp>

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
namespace core {

std::string FillFilePattern(const std::string& pathbase,
                            size_t worker, size_t file_part) {

    static const bool debug = false;

    using size_type = std::string::size_type;

    std::string out_path = pathbase;
    {
        // replace dollar
        size_type dollar_end = out_path.rfind('$');
        size_type dollar_begin = out_path.find_last_not_of('$', dollar_end);

        size_type dollar_length =
            dollar_end != std::string::npos && dollar_end > dollar_begin
            ? dollar_end - dollar_begin : 4;

        sLOG << "dollar_length" << dollar_length;
        out_path.replace(dollar_begin + 1, dollar_length,
                         common::str_snprintf<>(dollar_length + 2, "%0*zu",
                                                static_cast<int>(dollar_length),
                                                worker));
    }
    {
        // replace hash signs
        size_type hash_end = out_path.rfind('#');
        size_type hash_begin = out_path.find_last_not_of('#', hash_end);

        size_type hash_length =
            hash_end != std::string::npos && hash_end > hash_begin
            ? hash_end - hash_begin : 10;

        sLOG << "hash_length" << hash_length;
        out_path.replace(hash_begin + 1, hash_length,
                         common::str_snprintf<>(hash_length + 2, "%0*zu",
                                                static_cast<int>(hash_length),
                                                file_part));
    }
    return out_path;
}

std::vector<std::string> GlobFilePattern(const std::string& path) {

    std::vector<std::string> files;

#if defined(_MSC_VER)
    glob_local::CSimpleGlob sglob;
    sglob.Add(path.c_str());
    for (int n = 0; n < sglob.FileCount(); ++n) {
        files.emplace_back(sglob.File(n));
    }
#else
    glob_t glob_result;
    glob(path.c_str(), GLOB_TILDE, nullptr, &glob_result);

    for (unsigned int i = 0; i < glob_result.gl_pathc; ++i) {
        files.push_back(glob_result.gl_pathv[i]);
    }
    globfree(&glob_result);
#endif

    std::sort(files.begin(), files.end());

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

SysFile SysFile::OpenForRead(const std::string& path) {

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

        return SysFile(fd);
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

    return SysFile(pipefd[0], pid);
#endif
}

SysFile SysFile::OpenForWrite(const std::string& path) {

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

        return SysFile(fd);
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

    return SysFile(pipefd[1], pid);
#endif
}

/******************************************************************************/

#if defined(_MSC_VER)

std::string TemporaryDirectory::make_directory(const char* sample) {

    char temp_file_path[MAX_PATH + 1] = { 0 };
    unsigned success = ::GetTempFileName(".", sample, 0, temp_file_path);
    if (!success) {
        throw common::ErrnoException(
                  "Could not allocate temporary directory "
                  + std::string(temp_file_path));
    }

    if (!DeleteFile(temp_file_path)) {
        throw common::ErrnoException(
                  "Could not create temporary directory "
                  + std::string(temp_file_path));
    }

    if (!CreateDirectory(temp_file_path, nullptr)) {
        throw common::ErrnoException(
                  "Could not create temporary directory "
                  + std::string(temp_file_path));
    }

    return temp_file_path;
}

void TemporaryDirectory::wipe_directory(
    const std::string& tmp_dir, bool do_rmdir) {

    WIN32_FIND_DATA ff_data;
    HANDLE h = FindFirstFile((tmp_dir + "\\*").c_str(), &ff_data);

    if (h == INVALID_HANDLE_VALUE) {
        throw common::ErrnoException(
                  "FindFirstFile failed:" + std::to_string(GetLastError()));
    }

    do {
        if (!(ff_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
            std::string path = tmp_dir + "\\" + ff_data.cFileName;

            if (!DeleteFile(path.c_str())) {
                sLOG1 << "Could not unlink temporary file" << path
                      << ":" << strerror(errno);
            }
        }
    } while (FindNextFile(h, &ff_data) != 0);

    DWORD e = GetLastError();
    if (e != ERROR_NO_MORE_FILES) {
        throw common::ErrnoException(
                  "FindFirstFile failed:" + std::to_string(GetLastError()));
    }

    if (!do_rmdir) return;

    if (!RemoveDirectory(tmp_dir.c_str())) {
        throw common::ErrnoException(
                  "Could not remove temporary directory " + tmp_dir);
    }
}

#else

std::string TemporaryDirectory::make_directory(const char* sample) {

    std::string tmp_dir = std::string(sample) + "XXXXXX";
    // evil const_cast, but mkdtemp replaces the XXXXXX with something
    // unique. it also mkdirs.
    char* p = mkdtemp(const_cast<char*>(tmp_dir.c_str()));

    if (p == nullptr) {
        throw common::ErrnoException(
                  "Could create temporary directory " + tmp_dir);
    }

    return tmp_dir;
}

void TemporaryDirectory::wipe_directory(
    const std::string& tmp_dir, bool do_rmdir) {
    DIR* d = opendir(tmp_dir.c_str());
    if (d == nullptr) {
        throw common::ErrnoException(
                  "Could open temporary directory " + tmp_dir);
    }

    struct dirent* de, entry;
    while (readdir_r(d, &entry, &de) == 0 && de != nullptr) {
        // skip ".", "..", and also hidden files (don't create them).
        if (de->d_name[0] == '.') continue;

        std::string path = tmp_dir + "/" + de->d_name;
        int r = unlink(path.c_str());
        if (r != 0)
            sLOG1 << "Could not unlink temporary file" << path
                  << ":" << strerror(errno);
    }

    closedir(d);

    if (!do_rmdir) return;

    if (rmdir(tmp_dir.c_str()) != 0) {
        sLOG1 << "Could not unlink temporary directory" << tmp_dir
              << ":" << strerror(errno);
    }
}

#endif

} // namespace core
} // namespace thrill

/******************************************************************************/
