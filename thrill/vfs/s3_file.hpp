/*******************************************************************************
 * thrill/vfs/s3_file.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_VFS_S3_FILE_HEADER
#define THRILL_VFS_S3_FILE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/common/zip_stream.hpp>
#include <thrill/vfs/file_io.hpp>

#if THRILL_USE_AWS
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#endif

#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace vfs {

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

std::shared_ptr<S3File> S3OpenReadStream(
    const FileInfo& file, const api::Context& ctx,
    const common::Range& my_range, bool compressed);

std::shared_ptr<S3File> S3OpenWriteStream(
    const std::string& path, const api::Context& ctx);

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_S3_FILE_HEADER

/******************************************************************************/
