/*******************************************************************************
 * thrill/core/read_file_list.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_READ_FILE_LIST_HEADER
#define THRILL_CORE_READ_FILE_LIST_HEADER

#include <glob.h>
#include <sys/stat.h>

#include <thrill/common/string.hpp>
#include <thrill/common/logger.hpp>

namespace thrill {
namespace core {

using FileSizePair = std::pair<std::string, size_t>;

// Returns true, if file at filepath is compressed (e.g, ends with '.[gz/bz2,xz,lzo]')
bool IsCompressed(const std::string& path) {
    return common::ends_with(path, ".gz") ||
           common::ends_with(path, ".bz2") ||
           common::ends_with(path, ".xz") ||
           common::ends_with(path, ".lzo");
}

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

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_READ_FILE_LIST_HEADER

/******************************************************************************/
