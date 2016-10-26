/*******************************************************************************
 * thrill/vfs/file_io.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_VFS_FILE_IO_HEADER
#define THRILL_VFS_FILE_IO_HEADER

#include <thrill/common/counting_ptr.hpp>
#include <thrill/common/math.hpp>
#include <thrill/common/system_exception.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace vfs {

//! Initialize VFS layer
void Initialize();

//! Deinitialize VFS layer
void Deinitialize();

/******************************************************************************/

//! function which takes pathbase and replaces $$$ with worker and ### with
//! the file_part values.
std::string FillFilePattern(const std::string& pathbase,
                            size_t worker, size_t file_part);

//! Returns true, if file at filepath is compressed (e.g, ends with
//! '.{gz,bz2,xz,lzo}')
bool IsCompressed(const std::string& path);

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

//! List of file info and additional overall info.
struct FileList : public std::vector<FileInfo>{
    //! total size of files
    uint64_t total_size;

    //! whether the list contains a compressed file.
    bool     contains_compressed;
};

/*!
 * Reads a glob path list and deliver a file list, sizes, and prefixsums (in
 * bytes) for all matching files.
 */
FileList Glob(const std::string& glob);

/*!
 * Reads a glob path list and deliver a file list, sizes, and prefixsums (in
 * bytes) for all matching files.
 */
FileList Glob(const std::vector<std::string>& globlist);

/******************************************************************************/

class ReadStream : public virtual common::ReferenceCount
{
public:
    virtual ~ReadStream() { }

    virtual ssize_t read(void* data, size_t size) = 0;

    virtual ssize_t lseek(off_t) = 0;

    virtual void close() = 0;
};

class WriteStream : public virtual common::ReferenceCount
{
public:
    virtual ~WriteStream() { }

    virtual ssize_t write(const void* data, size_t size) = 0;

    virtual void close() = 0;
};

using ReadStreamPtr = common::CountingPtr<ReadStream>;
using WriteStreamPtr = common::CountingPtr<WriteStream>;

/******************************************************************************/

ReadStreamPtr OpenReadStream(
    const std::string& path, const common::Range& range);

WriteStreamPtr OpenWriteStream(const std::string& path);

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_FILE_IO_HEADER

/******************************************************************************/
