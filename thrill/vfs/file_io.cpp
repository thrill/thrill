/*******************************************************************************
 * thrill/vfs/file_io.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/file_io.hpp>

#if THRILL_USE_AWS
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#endif

#include <thrill/api/context.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/vfs/s3_file.hpp>
#include <thrill/vfs/simple_glob.hpp>
#include <thrill/vfs/sys_file.hpp>

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
    }
    else {

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
        std::vector<std::string> list = GlobFilePattern(path);
        if (list.size() == 0)
            throw std::runtime_error("No files found matching file/glob: " + path);
        filelist.insert(filelist.end(), list.begin(), list.end());
    }
    return filelist;
}

FileList GlobFileSizePrefixSum(api::Context& ctx, const std::vector<std::string>& files) {

    std::vector<FileInfo> file_info;
    struct stat filestat;
    uint64_t total_size = 0;
    bool contains_compressed = false;

    for (const std::string& file : files) {

        if (common::StartsWith(file, "s3://")) {

#if !THRILL_USE_AWS

            throw std::runtime_error("THRILL_USE_AWS is not set to true");

#else

            auto s3_client = ctx.s3_client();

            std::string path_without_s3 = file.substr(5);

            std::vector<std::string> splitted = common::Split(
                path_without_s3, '/', (std::string::size_type)2);
            Aws::S3::Model::ListObjectsRequest lor;
            lor.SetBucket(splitted[0]);

            if (splitted.size() == 2) {
                lor.SetPrefix(splitted[1]);
            }

            auto loo = s3_client->ListObjects(lor);
            if (!loo.IsSuccess()) {
                LOG1 << "Error message: " << loo.GetError().GetMessage();
                throw std::runtime_error("No file found in bucket \"" + splitted[0] + "\" with correct key");
            }

            for (const auto& object : loo.GetResult().GetContents()) {
                LOG1 << "file:" << object.GetKey();
                if (object.GetSize() > 0) {
                    // folders are also in this list but have size of 0
                    file_info.emplace_back(FileInfo {
                                               std::string("s3://").append(splitted[0]).append("/")
                                               .append(object.GetKey()),
                                               static_cast<uint64_t>(object.GetSize()),
                                               total_size
                                           });

                    contains_compressed = contains_compressed ||
                                          common::EndsWith(object.GetKey(), ".gz");

                    total_size += object.GetSize();
                }
            }
#endif
        }
        else {

            if (stat(file.c_str(), &filestat)) {
                throw std::runtime_error(
                          "ERROR: Invalid file " + std::string(file));
            }
            if (!S_ISREG(filestat.st_mode)) continue;

            contains_compressed = contains_compressed || IsCompressed(file);

            file_info.emplace_back(
                FileInfo { std::move(file),
                           static_cast<uint64_t>(filestat.st_size), total_size });

            total_size += filestat.st_size;
        }
    }

    // sentinel entry
    file_info.emplace_back(
        FileInfo { std::string(),
                   static_cast<uint64_t>(0), total_size });

    return FileList {
               std::move(file_info), total_size, contains_compressed
    };
}

/******************************************************************************/

ReadStreamPtr OpenReadStream(
    const FileInfo& file, const api::Context& ctx,
    const common::Range& range) {
    if (common::StartsWith(file.path, "s3://")) {
        return S3OpenReadStream(file, ctx, range);
    }
    else {
        return SysOpenReadStream(file.path);
    }
}

WriteStreamPtr OpenWriteStream(
    const std::string& path, const api::Context& ctx) {
    if (common::StartsWith(path, "s3://")) {
        return S3OpenWriteStream(path, ctx);
    }
    else {
        return SysOpenWriteStream(path);
    }
}

} // namespace vfs
} // namespace thrill

/******************************************************************************/
