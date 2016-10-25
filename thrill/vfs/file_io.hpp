/*******************************************************************************
 * thrill/vfs/file_io.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_VFS_FILE_IO_HEADER
#define THRILL_VFS_FILE_IO_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/counting_ptr.hpp>
#include <thrill/common/system_exception.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace vfs {

//! function which takes pathbase and replaces $$$ with worker and ### with
//! the file_part values.
std::string FillFilePattern(const std::string& pathbase,
                            size_t worker, size_t file_part);

// Returns true, if file at filepath is compressed (e.g, ends with
// '.{gz,bz2,xz,lzo}')
static inline
bool IsCompressed(const std::string& path) {
    return common::EndsWith(path, ".gz") ||
           common::EndsWith(path, ".bz2") ||
           common::EndsWith(path, ".xz") ||
           common::EndsWith(path, ".lzo") ||
           common::EndsWith(path, ".lz4");
}

//! General information of system file.
struct FileInfo {
    //! path to file
    std::string path;
    //! size of file.
    uint64_t    size;
    //! exclusive prefix sum of file sizes.
    uint64_t    size_ex_psum;

    //! inclusive prefix sum of file sizes.
    uint64_t    size_inc_psum() const { return size_ex_psum + size; }
    //! if the file is compressed
    bool        IsCompressed() const { return vfs::IsCompressed(path); }
};

//! List of file info and overall info.
struct FileList {
    //! list of files.
    std::vector<FileInfo> list;

    //! number of files, list.size() - 1.
    size_t                count() const { return list.size() - 1; }

    //! total size of files
    uint64_t              total_size;

    //! whether the list contains a compressed file.
    bool                  contains_compressed;
};

/*!
 * Reads a path as a file list contains, sizes and prefixsums (in bytes) for all
 * files in the input path.
 */
FileList GlobFileSizePrefixSum(
    api::Context& ctx, const std::vector<std::string>& files);

//! Returns a vector of all files found by glob in the input path.
std::vector<std::string> GlobFilePattern(const std::string& path);

//! Returns a vector of all files found by glob in the input path.
std::vector<std::string> GlobFilePatterns(
    const std::vector<std::string>& globlist);

/******************************************************************************/

class ReadStream : public virtual common::ReferenceCount
{
public:
    virtual ~ReadStream() { }

    virtual ssize_t read(void*, size_t) = 0;

    virtual ssize_t lseek(off_t) = 0;

    virtual void close() = 0;
};

class WriteStream : public virtual common::ReferenceCount
{
public:
    virtual ~WriteStream() { }

    virtual ssize_t write(const void*, size_t) = 0;

    virtual void close() = 0;
};

using ReadStreamPtr = common::CountingPtr<ReadStream>;
using WriteStreamPtr = common::CountingPtr<WriteStream>;

/******************************************************************************/

ReadStreamPtr OpenReadStream(
    const std::string& path, const api::Context& ctx,
    const common::Range& range);

WriteStreamPtr OpenWriteStream(
    const std::string& path, const api::Context& ctx);

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_FILE_IO_HEADER

/******************************************************************************/
