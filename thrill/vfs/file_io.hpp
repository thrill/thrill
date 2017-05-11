/*******************************************************************************
 * thrill/vfs/file_io.hpp
 *
 * Abstract interfaces of virtual file system (VFS) layer
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

#include <thrill/common/math.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/common/system_exception.hpp>
#include <tlx/counting_ptr.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace vfs {

/******************************************************************************/

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

//! Returns true, if file at filepath is a remote uri like s3:// or hdfs://
bool IsRemoteUri(const std::string& path);

//! VFS object type
enum class Type { File, Directory };

std::ostream& operator << (std::ostream& os, const Type& t);

//! General information of vfs file.
struct FileInfo {
    //! type of entry
    Type        type;
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
    //! if the file is at remote uri
    bool        IsRemoteUri() const { return vfs::IsRemoteUri(path); }
};

//! List of file info and additional overall info.
struct FileList : public std::vector<FileInfo>{
    //! total size of files
    uint64_t total_size;

    //! whether the list contains a compressed file.
    bool     contains_compressed;

    //! whether the list contains a remote-uri file.
    bool     contains_remote_uri;

    //! inclusive prefix sum of file sizes (only for symmetry with ex_psum)
    uint64_t size_inc_psum(size_t i) const
    { return operator [] (i).size_inc_psum(); }

    //! exclusive prefix sum of file sizes with total_size as sentinel
    uint64_t size_ex_psum(size_t i) const
    { return i < size() ? operator [] (i).size_ex_psum : total_size; }
};

//! Type of objects to include in glob result.
enum class GlobType { All, File, Directory };

/*!
 * Reads a glob path list and deliver a file list, sizes, and prefixsums (in
 * bytes) for all matching files.
 */
FileList Glob(const std::string& glob, const GlobType& gtype = GlobType::All);

/*!
 * Reads a glob path list and deliver a file list, sizes, and prefixsums (in
 * bytes) for all matching files.
 */
FileList Glob(const std::vector<std::string>& globlist,
              const GlobType& gtype = GlobType::All);

/******************************************************************************/

/*!
 * Reader object from any source. Streams can be created for any supported URI
 * and seek to the given range's offset.
 */
class ReadStream : public virtual tlx::ReferenceCounter
{
public:
    virtual ~ReadStream();

    //! read up to size bytes from stream.
    virtual ssize_t read(void* data, size_t size) = 0;

    //! close stream, release resources.
    virtual void close() = 0;
};

/*!
 * Writer object to output data to any supported URI.
 */
class WriteStream : public virtual tlx::ReferenceCounter
{
public:
    virtual ~WriteStream();

    virtual ssize_t write(const void* data, size_t size) = 0;

    virtual void close() = 0;
};

using ReadStreamPtr = tlx::CountingPtr<ReadStream>;
using WriteStreamPtr = tlx::CountingPtr<WriteStream>;

/******************************************************************************/

/*!
 * Construct reader for given path uri. Range is the byte range [b,e) inside the
 * file to read. If e = 0, the complete file is read.
 *
 * For the POSIX SysFile implementation the range is used only to seek to the
 * byte offset b. It allows additional bytes after e to be read.
 *
 * For the S3File implementations, however, the range[b,e) is used to determine
 * which data to fetch from S3. Hence, once e is reached, read() will return
 * EOF.
 */
ReadStreamPtr OpenReadStream(
    const std::string& path, const common::Range& range = common::Range());

WriteStreamPtr OpenWriteStream(const std::string& path);

/******************************************************************************/

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_FILE_IO_HEADER

/******************************************************************************/
