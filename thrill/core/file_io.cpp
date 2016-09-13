/*******************************************************************************
 * thrill/core/file_io.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#if THRILL_USE_AWS
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#endif

#include <thrill/api/context.hpp>
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

    static constexpr bool debug = false;

    using size_type = std::string::size_type;

    std::string out_path = pathbase;
    {
        // replace @
        size_type at_end = out_path.rfind('@');
        size_type at_begin = out_path.find_last_not_of('@', at_end);

        size_type at_length =
            at_end != std::string::npos && at_end > at_begin
            ? at_end - at_begin : 4;

        sLOG << "at_length" << at_length;
        out_path.replace(at_begin + 1, at_length,
                         common::str_snprintf<>(at_length + 2, "%0*zu",
                                                static_cast<int>(at_length),
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

    if (common::StartsWith(path, "s3://")) {
        files.push_back(path);
    } else {

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

    }

    std::sort(files.begin(), files.end());

    return files;
}

std::vector<std::string> GlobFilePatterns(
    const std::vector<std::string>& globlist) {

    std::vector<std::string> filelist;
    for (const std::string& path : globlist) {
        std::vector<std::string> list = core::GlobFilePattern(path);
        if (list.size() == 0)
            throw std::runtime_error("No files found matching file/glob: " + path);
        filelist.insert(filelist.end(), list.begin(), list.end());
    }
    return filelist;
}

SysFileList GlobFileSizePrefixSum(const std::vector<std::string>& files,
                                  api::Context& ctx) {

    std::vector<SysFileInfo> file_info;
    struct stat filestat;
    uint64_t total_size = 0;
    bool contains_compressed = false;

    for (const std::string& file : files) {


        if (common::StartsWith(file, "s3://")) {

#if !THRILL_USE_AWS
            throw std::runtime_error("THRILL_USE_AWS is not set to true");
#endif
            auto s3_client = ctx.s3_client();

            std::string path_without_s3 = file.substr(5);

            std::vector<std::string> splitted = common::Split(
                path_without_s3, '/', (std::string::size_type) 2);
            Aws::S3::Model::ListObjectsRequest lor;
            lor.SetBucket(splitted[0]);

            if (splitted.size() == 2) {
                lor.SetPrefix(splitted[1]);
            }

            auto loo = s3_client->ListObjects(lor);
            if (!loo.IsSuccess()) {
                throw std::runtime_error("No file found in bucket " + file);
            }

            for (const auto& object : loo.GetResult().GetContents()) {
                if (object.GetSize() > 0) {
                    //folders are also in this list but have size of 0
                    file_info.emplace_back(SysFileInfo {
                            std::string("s3://").append(splitted[0]).append("/")
                                .append(object.GetKey()),
                                static_cast<uint64_t>(object.GetSize()),
                                total_size});

                    total_size += object.GetSize();
                }
            }
        } else {

            if (stat(file.c_str(), &filestat)) {
                throw std::runtime_error(
                    "ERROR: Invalid file " + std::string(file));
            }
            if (!S_ISREG(filestat.st_mode)) continue;

            contains_compressed = contains_compressed || IsCompressed(file);

            file_info.emplace_back(
                SysFileInfo { std::move(file),
                        static_cast<uint64_t>(filestat.st_size), total_size });

            total_size += filestat.st_size;
        }
    }

    // sentinel entry
    file_info.emplace_back(
        SysFileInfo { std::string(),
                      static_cast<uint64_t>(0), total_size });

    return SysFileList {
               std::move(file_info), total_size, contains_compressed
    };
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


std::shared_ptr<SysFile> SysFile::OpenForRead(const std::string& path) {

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

        return std::make_shared<core::SysFile>(SysFile(fd));
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

    return std::make_shared<core::SysFile>(SysFile(pipefd[0], pid));
#endif
}

std::shared_ptr<SysFile> SysFile::OpenForWrite(const std::string& path) {

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

        return std::make_shared<core::SysFile>(SysFile(fd));
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

    return std::make_shared<core::SysFile>(SysFile(pipefd[1], pid));
#endif
}

std::shared_ptr<S3File> S3File::OpenForRead(const SysFileInfo& file,
                                            const api::Context& ctx,
                                            const common::Range& my_range) {

    //Amount of additional bytes read after end of range
    size_t maximum_line_length = 64 * 1024;

    Aws::S3::Model::GetObjectRequest getObjectRequest;

    std::string path_without_s3 = file.path.substr(5);

    std::vector<std::string> splitted = common::Split(
        path_without_s3, '/', (std::string::size_type) 2);

    assert(splitted.size() == 2);

    getObjectRequest.SetBucket(splitted[0]);
    getObjectRequest.SetKey(splitted[1]);

    LOG << "Attempting to read from bucket " << splitted[0] << " with key "
        << splitted[1] << "!";

    std::string range = "bytes=";
    bool use_range_ = false;
    size_t range_start = 0;
    if (my_range.begin > file.size_ex_psum) {
        range += std::to_string(my_range.begin - file.size_ex_psum);
        range_start = my_range.begin - file.size_ex_psum;
        use_range_ = true;
    } else {
        range += "0";
    }

    range += "-";
    if (my_range.end + maximum_line_length < file.size_inc_psum()) {
        range += std::to_string(file.size - (file.size_inc_psum() -
                                             my_range.end -
                                             maximum_line_length));
        use_range_ = true;
    }

    if (use_range_)
        getObjectRequest.SetRange(range);

    auto outcome = ctx.s3_client()->GetObject(getObjectRequest);

    if (!outcome.IsSuccess())
        throw common::ErrnoException(
            "Download from S3 Errored: " + outcome.GetError().GetMessage());

    return std::make_shared<S3File>(outcome.GetResultWithOwnership(), range_start);

}

std::shared_ptr<S3File> S3File::OpenForWrite(const std::string& path,
                                             const api::Context& ctx) {
        return std::make_shared<S3File>(ctx.s3_client(), path);
}

std::shared_ptr<AbstractFile> AbstractFile::OpenForRead(const SysFileInfo& file,
                                                        const api::Context& ctx,
                                                        const common::Range&
                                                        my_range) {
    if (common::StartsWith(file.path, "s3://")) {
        return S3File::OpenForRead(file, ctx, my_range);
    } else {
        return SysFile::OpenForRead(file.path);
    }
}

std::shared_ptr<AbstractFile> AbstractFile::OpenForWrite(
    const std::string& path, const api::Context& ctx) {
    if (common::StartsWith(path, "s3://")) {
        return S3File::OpenForWrite(path, ctx);
    } else {
        return SysFile::OpenForWrite(path);
    }
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
