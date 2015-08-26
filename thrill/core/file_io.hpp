/*******************************************************************************
 * thrill/core/file_io.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_FILE_IO_HEADER
#define THRILL_CORE_FILE_IO_HEADER

#include <glob.h>
#include <sys/stat.h>

#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

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
               common::ends_with(path, ".lzo");
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

    //! Open file and return file handle
    //! \param path Path to open
    int OpenFile(const std::string& path) {

        // path too short, can't end with .[gz/bz2,xz,lzo]
        if (path.size() < 4) return open(path.c_str(), O_RDONLY);

        const char* decompressor = nullptr;

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

        // not a compressed file
        if (!decompressor) return open(path.c_str(), O_RDONLY);

        // create pipe, fork and call decompressor as child
        int pipefd[2];                     // pipe[0] = read, pipe[1] = write
        if (pipe(pipefd) != 0) {
            LOG1 << "Error creating pipe: " << strerror(errno);
            exit(-1);
        }

        pid_t pid = fork();
        if (pid == 0) {
            close(pipefd[0]);                                           // close read end
            dup2(pipefd[1], STDOUT_FILENO);                             // replace stdout with pipe

            execlp(decompressor, decompressor, "-dc", path.c_str(), nullptr);

            LOG1 << "Pipe execution failed: " << strerror(errno);
            close(pipefd[1]);                     // close write end
            exit(-1);
        }

        close(pipefd[1]);                         // close write end

        return pipefd[0];
    }
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_FILE_IO_HEADER

/******************************************************************************/
