/*******************************************************************************
 * thrill/vfs/file_io.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/file_io.hpp>

#include <thrill/common/die.hpp>
#include <thrill/common/string.hpp>
#include <thrill/vfs/bzip2_filter.hpp>
#include <thrill/vfs/gzip_filter.hpp>
#include <thrill/vfs/s3_file.hpp>
#include <thrill/vfs/sys_file.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace thrill {
namespace vfs {

/******************************************************************************/

void Initialize() {
    S3Initialize();
}

void Deinitialize() {
    S3Deinitialize();
}

/******************************************************************************/

bool IsCompressed(const std::string& path) {
    return common::EndsWith(path, ".gz") ||
           common::EndsWith(path, ".bz2") ||
           common::EndsWith(path, ".xz") ||
           common::EndsWith(path, ".lzo") ||
           common::EndsWith(path, ".lz4");
}

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

/******************************************************************************/

FileList Glob(const std::vector<std::string>& globlist) {
    FileList filelist;

    // run through globs and collect files. The sub-Glob() methods must only
    // fill in the fields "path" and "size" of FileInfo, overall stats are
    // calculated afterwards.
    for (const std::string& path : globlist)
    {
        if (common::StartsWith(path, "file://")) {
            // remove the file:// prefix
            SysGlob(path.substr(7), filelist);
        }
        else if (common::StartsWith(path, "s3://")) {
            S3Glob(path, filelist);
        }
        else {
            SysGlob(path, filelist);
        }
    }

    // calculate exclusive prefix sum and overall stats

    filelist.contains_compressed = false;
    filelist.total_size = 0;
    uint64_t size_ex_psum = 0;

    for (FileInfo& fi : filelist)
    {
        uint64_t size_next = size_ex_psum + fi.size;
        fi.size_ex_psum = size_ex_psum;
        size_ex_psum = size_next;

        filelist.contains_compressed |= fi.IsCompressed();
        filelist.total_size += fi.size;
    }

    return filelist;
}

FileList Glob(const std::string& glob) {
    return Glob(std::vector<std::string>{ glob });
}

/******************************************************************************/

ReadStreamPtr OpenReadStream(
    const std::string& path, const common::Range& range) {

    ReadStreamPtr p;
    if (common::StartsWith(path, "file://")) {
        p = SysOpenReadStream(path.substr(7), range);
    }
    else if (common::StartsWith(path, "s3://")) {
        p = S3OpenReadStream(path, range);
    }
    else {
        p = SysOpenReadStream(path, range);
    }

    if (common::EndsWith(path, ".gz")) {
        p = MakeGZipReadFilter(p);
        die_unless(range.begin == 0 || "Cannot seek in compressed streams.");
    }
    else if (common::EndsWith(path, ".bz2")) {
        p = MakeBZip2ReadFilter(p);
        die_unless(range.begin == 0 || "Cannot seek in compressed streams.");
    }

    return p;
}

WriteStreamPtr OpenWriteStream(const std::string& path) {

    WriteStreamPtr p;
    if (common::StartsWith(path, "file://")) {
        p = SysOpenWriteStream(path.substr(7));
    }
    else if (common::StartsWith(path, "s3://")) {
        p = S3OpenWriteStream(path);
    }
    else {
        p = SysOpenWriteStream(path);
    }

    if (common::EndsWith(path, ".gz")) {
        p = MakeGZipWriteFilter(p);
    }
    else if (common::EndsWith(path, ".bz2")) {
        p = MakeBZip2WriteFilter(p);
    }

    return p;
}

} // namespace vfs
} // namespace thrill

/******************************************************************************/
