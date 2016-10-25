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
#include <thrill/common/logger.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/common/zip_stream.hpp>

#if THRILL_USE_AWS
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#endif

#if defined(_MSC_VER)

#include <io.h>

#else

#include <unistd.h>

#endif

#include <string>
#include <utility>
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
struct SysFileInfo {
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
struct SysFileList {
    //! list of files.
    std::vector<SysFileInfo> list;

    //! number of files, list.size() - 1.
    size_t                   count() const { return list.size() - 1; }

    //! total size of files
    uint64_t                 total_size;

    //! whether the list contains a compressed file.
    bool                     contains_compressed;
};

/*!
 * Reads a path as a file list contains, sizes and prefixsums (in bytes) for all
 * files in the input path.
 */
SysFileList GlobFileSizePrefixSum(const std::vector<std::string>& files,
                                  api::Context& ctx);

//! Returns a vector of all files found by glob in the input path.
std::vector<std::string> GlobFilePattern(const std::string& path);

//! Returns a vector of all files found by glob in the input path.
std::vector<std::string> GlobFilePatterns(
    const std::vector<std::string>& globlist);

class AbstractFile
{
public:
    static std::shared_ptr<AbstractFile> OpenForRead(const SysFileInfo& file,
                                                     const api::Context& ctx,
                                                     const common::Range& my_range,
                                                     bool compressed);

    static std::shared_ptr<AbstractFile> OpenForWrite(const std::string& path,
                                                      const api::Context& ctx);

    virtual ssize_t write(const void*, size_t) = 0;

    virtual ssize_t read(void*, size_t) = 0;

    virtual ssize_t lseek(off_t) = 0;

    virtual void close() = 0;
};

class S3File : public AbstractFile
{

    static constexpr bool debug = false;

public:
    S3File() : is_valid_(false), is_read_(false) { }

    S3File(Aws::S3::Model::GetObjectResult&& gor, size_t range_start)
        : gor_(std::move(gor)), range_start_(range_start),
          is_valid_(true), is_read_(true), decompression_(false) { }

    S3File(Aws::S3::Model::GetObjectResult&& gor)
        : gor_(std::move(gor)), is_valid_(true), is_read_(true),
          decompression_(true) {

        unzip_ = std::make_unique<common::zip_istream>(gor_.GetBody());
    }

    S3File(std::shared_ptr<Aws::S3::S3Client> client, const std::string& path)
        : client_(client), path_(path), is_valid_(true), is_read_(false) { }

    //! non-copyable: delete copy-constructor
    S3File(const S3File&) = delete;
    //! non-copyable: delete assignment operator
    S3File& operator = (const S3File&) = delete;
    //! move-constructor

    S3File(S3File&& f) noexcept
        : gor_(std::move(f.gor_)), write_stream_(std::move(f.write_stream_)),
          client_((f.client_)), path_(f.path_), is_valid_(f.is_valid_),
          is_read_(f.is_read_) {
        assert(0);
        f.is_valid_ = false;
    }

    static std::shared_ptr<S3File> OpenForRead(const SysFileInfo& file,
                                               const api::Context& ctx,
                                               const common::Range& my_range,
                                               bool compressed);

    static std::shared_ptr<S3File> OpenForWrite(const std::string& path,
                                                const api::Context& ctx);

    ssize_t write(const void* data, size_t count) {
        assert(is_valid_);
        assert(!is_read_);
        ssize_t before = write_stream_.tellp();
        write_stream_.write((const char*)data, count);
        return write_stream_.tellp() - before;
    }

    //! POSIX read function.
    ssize_t read(void* data, size_t count) {
        LOG1 << "readcount " << count;
        assert(is_valid_);
        assert(is_read_);
        if (decompression_) {
            size_t sizet = unzip_->readsome((char*)data, count);
            LOG1 << "YOOO " << sizet;
            return sizet;
        }
        else {
            return gor_.GetBody().readsome((char*)data, count);
        }
    }

    //! Emulates a seek. Due to HTTP Range requests we only load the file from
    //! first byte in local range. Therefore we don't need to actually seek and
    //! only have to emulate it by returning the 'seeked' offset.
    ssize_t lseek(off_t offset) {
        assert(is_valid_);
        assert(is_read_);
        assert(offset == (off_t)range_start_);
        return offset;
    }

    void close() {
        if (!is_read_ && is_valid_) {
            LOG << "Closing write file, uploading";
            Aws::S3::Model::PutObjectRequest por;

            std::string path_without_s3 = path_.substr(5);
            std::vector<std::string> splitted = common::Split(
                path_without_s3, '/', (std::string::size_type)2);

            Aws::S3::Model::CreateBucketRequest createBucketRequest;
            createBucketRequest.SetBucket(splitted[0]);

            client_->CreateBucket(createBucketRequest);

            por.SetBucket(splitted[0]);
            por.SetKey(splitted[1]);
            std::shared_ptr<Aws::IOStream> stream = std::make_shared<Aws::IOStream>(
                write_stream_.rdbuf());
            por.SetBody(stream);
            por.SetContentLength(write_stream_.tellp());
            auto outcome = client_->PutObject(por);
            if (!outcome.IsSuccess()) {
                throw common::ErrnoException(
                          "Download from S3 Errored: " + outcome.GetError().GetMessage());
            }
        }

        is_valid_ = false;
    }

private:
    Aws::S3::Model::GetObjectResult gor_;
    std::unique_ptr<common::zip_istream> unzip_;

    Aws::StringStream write_stream_;
    std::shared_ptr<Aws::S3::S3Client> client_;
    std::string path_;

    size_t range_start_;

    bool is_valid_;
    bool is_read_;
    bool decompression_;
};

/*!
 * Represents a POSIX system file via its file descriptor.
 */
class SysFile : public AbstractFile
{
    static constexpr bool debug = false;

public:
    //! default constructor
    SysFile() : fd_(-1) { }

    /*!
     * Open file for reading and return file descriptor. Handles compressed
     * files by calling a decompressor in a pipe, like "cat $f | gzip -dc |" in
     * bash.
     *
     * \param path Path to open
     */
    static std::shared_ptr<SysFile> OpenForRead(const std::string& path);

    /*!
     * Open file for writing and return file descriptor. Handles compressed
     * files by calling a compressor in a pipe, like "| gzip -d > $f" in bash.
     *
     * \param path Path to open
     */
    static std::shared_ptr<SysFile> OpenForWrite(const std::string& path);

    //! non-copyable: delete copy-constructor
    SysFile(const SysFile&) = delete;
    //! non-copyable: delete assignment operator
    SysFile& operator = (const SysFile&) = delete;
    //! move-constructor
    SysFile(SysFile&& f) noexcept
        : fd_(f.fd_), pid_(f.pid_) {
        f.fd_ = -1, f.pid_ = 0;
    }
    //! move-assignment
    SysFile& operator = (SysFile&& f) {
        close();
        fd_ = f.fd_, pid_ = f.pid_;
        f.fd_ = -1, f.pid_ = 0;
        return *this;
    }

    //! POSIX write function.
    ssize_t write(const void* data, size_t count) {
        assert(fd_ >= 0);
#if defined(_MSC_VER)
        return ::_write(fd_, data, static_cast<unsigned>(count));
#else
        return ::write(fd_, data, count);
#endif
    }

    //! POSIX read function.
    ssize_t read(void* data, size_t count) {
        assert(fd_ >= 0);
#if defined(_MSC_VER)
        return ::_read(fd_, data, static_cast<unsigned>(count));
#else
        return ::read(fd_, data, count);
#endif
    }

    //! POSIX lseek function from current position.
    ssize_t lseek(off_t offset) {
        assert(fd_ >= 0);
        return ::lseek(fd_, offset, SEEK_CUR);
    }

    //! close the file descriptor
    void close();

    ~SysFile() {
        close();
    }

private:
    //! private constructor: use OpenForRead or OpenForWrite.
    explicit SysFile(int fd, int pid = 0) noexcept
        : fd_(fd), pid_(pid) { }

    //! file descriptor
    int fd_ = -1;

#if defined(_MSC_VER)
    using pid_t = int;
#endif

    //! pid of child process to wait for
    pid_t pid_ = 0;
};

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_FILE_IO_HEADER

/******************************************************************************/
