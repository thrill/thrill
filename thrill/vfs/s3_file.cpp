/*******************************************************************************
 * thrill/vfs/s3_file.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/s3_file.hpp>

#include <thrill/common/die.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

#include <libs3.h>

#include <algorithm>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace vfs {

// flag to output debug info from S3
static constexpr bool debug = false;

#if THRILL_HAVE_LIBS3

/******************************************************************************/

void S3Initialize() {
    S3_initialize(
        /* userAgentInfo */ nullptr, S3_INIT_ALL,
        /* defaultS3Hostname */ nullptr);
}

void S3Deinitialize() {
    S3_deinitialize();
}

/******************************************************************************/

//! Generic S3 error logger
void LibS3LogError(S3Status status, const S3ErrorDetails* error) {

    if (status != S3StatusOK) {
        LOG1 << "S3-ERROR - Status: " << S3_get_status_name(status);
    }

    if (error && error->message) {
        LOG1 << "S3-ERROR - Message: " << error->message;
    }
    if (error && error->resource) {
        LOG1 << "S3-ERROR - Resource: " << error->resource;
    }
    if (error && error->furtherDetails) {
        LOG1 << "S3-ERROR - Further Details: " << error->furtherDetails;
    }
    if (error && error->extraDetailsCount) {
        LOG1 << "S3-ERROR - Extra Details:";
        for (int i = 0; i < error->extraDetailsCount; i++) {
            LOG1 << "S3-ERROR - - " << error->extraDetails[i].name
                 << ": " << error->extraDetails[i].value;
        }
    }
}

//! Generic logger which outputs S3 properties
S3Status ResponsePropertiesCallback(
    const S3ResponseProperties* properties, void* /* callbackData */) {

    if (!debug) return S3StatusOK;

    if (properties->contentType)
        LOG1 << "S3-DEBUG - Content-Type: " << properties->contentType;
    if (properties->requestId)
        LOG1 << "S3-DEBUG - Request-Id: " << properties->requestId;
    if (properties->requestId2)
        LOG1 << "S3-DEBUG - Request-Id-2: " << properties->requestId2;
    if (properties->contentLength > 0)
        LOG1 << "S3-DEBUG - Content-Length: " << properties->contentLength;
    if (properties->server)
        LOG1 << "S3-DEBUG - Server: " << properties->server;
    if (properties->eTag)
        LOG1 << "S3-DEBUG - ETag: " << properties->eTag;
    if (properties->lastModified > 0) {
        char timebuf[256];
        time_t t = (time_t)properties->lastModified;
        struct tm tm;
        memset(&tm, 0, sizeof(tm));
        // gmtime is not thread-safe but we don't care here.
        strftime(timebuf, sizeof(timebuf),
                 "%Y-%m-%dT%H:%M:%SZ", localtime_r(&t, &tm));
        LOG1 << "S3-DEBUG - Last-Modified: " << timebuf;
    }
    for (int i = 0; i < properties->metaDataCount; i++) {
        LOG1 << "S3-DEBUG - x-amz-meta-" << properties->metaData[i].name
             << ": " << properties->metaData[i].value;
    }

    return S3StatusOK;
}

/******************************************************************************/
// Helper Methods

//! fill in a S3BucketContext
static void FillS3BucketContext(S3BucketContext& bkt, const std::string& key) {
    memset(&bkt, 0, sizeof(bkt));

    bkt.hostName = getenv("THRILL_S3_HOST");
    bkt.bucketName = key.c_str();
    bkt.protocol = S3ProtocolHTTPS;
    bkt.uriStyle = S3UriStyleVirtualHost;
    bkt.accessKeyId = getenv("THRILL_S3_KEY");
    bkt.secretAccessKey = getenv("THRILL_S3_SECRET");

    if (!bkt.accessKeyId) {
        die("S3-ERROR - set environment variable THRILL_S3_KEY");
    }
    if (!bkt.secretAccessKey) {
        die("S3-ERROR - set environment variable THRILL_S3_SECRET");
    }
}

/******************************************************************************/
// List Bucket Contents on S3

class LibS3ListBucket
{
public:
    bool list_bucket(const S3BucketContext* bucket_context,
                     const char* prefix, const char* marker = nullptr,
                     const char* delimiter = nullptr,
                     int maxkeys = std::numeric_limits<int>::max()) {

        // construct handlers
        S3ListBucketHandler handlers;
        memset(&handlers, 0, sizeof(handlers));

        handlers.responseHandler.propertiesCallback =
            &ResponsePropertiesCallback;
        handlers.responseHandler.completeCallback =
            &LibS3ListBucket::ResponseCompleteCallback;
        handlers.listBucketCallback = &LibS3ListBucket::ListBucketCallback;

        // loop until all keys were received
        status_ = S3StatusOK;
        last_marker_ = marker;
        is_truncated_ = false;

        do {
            S3_list_bucket(
                bucket_context, prefix, last_marker_, delimiter, maxkeys,
                /* requestContext */ nullptr, &handlers, this);
        } while (status_ == S3StatusOK && is_truncated_);      // NOLINT

        return (status_ == S3StatusOK);
    }

private:
    //! status of request
    S3Status status_ = S3StatusOK;

    //! last key seen
    const char* last_marker_;

    //! if result is truncated, issue next request
    bool is_truncated_ = false;

    //! completion callback, check for errors
    void ResponseCompleteCallback(
        S3Status status, const S3ErrorDetails* error) {

        status_ = status;
        if (status != S3StatusOK)
            LibS3LogError(status, error);
    }

    //! static wrapper to call
    static void ResponseCompleteCallback(
        S3Status status, const S3ErrorDetails* error, void* callbackData) {
        LibS3ListBucket* t = reinterpret_cast<LibS3ListBucket*>(callbackData);
        return t->ResponseCompleteCallback(status, error);
    }

    //! callback delivered list
    S3Status ListBucketCallback(
        int isTruncated, const char* /* nextMarker */, int contentsCount,
        const S3ListBucketContent* contents, int commonPrefixesCount,
        const char** commonPrefixes) {
        for (int i = 0; i < contentsCount; ++i) {
            LOG1 << "ListBucketCallback - " << contents[i].key;
        }
        for (int i = 0; i < commonPrefixesCount; ++i) {
            LOG1 << "Prefix: " << commonPrefixes[i];
        }
        last_marker_ = contents[contentsCount - 1].key;
        is_truncated_ = isTruncated;
        return S3StatusOK;
    }

    //! static wrapper to call ListBucketCallback
    static S3Status ListBucketCallback(
        int isTruncated, const char* nextMarker, int contentsCount,
        const S3ListBucketContent* contents, int commonPrefixesCount,
        const char** commonPrefixes, void* callbackData) {
        LibS3ListBucket* t = reinterpret_cast<LibS3ListBucket*>(callbackData);
        return t->ListBucketCallback(
            isTruncated, nextMarker, contentsCount, contents,
            commonPrefixesCount, commonPrefixes);
    }
};

/******************************************************************************/
// Stream Reading from S3

class S3ReadStream : public vfs::ReadStream
{
public:
    S3ReadStream(const S3BucketContext* bucket_context, const char* key,
                 const S3GetConditions* getConditions,
                 uint64_t startByte, uint64_t byteCount) {

        // construct handlers
        S3GetObjectHandler handler;
        memset(&handler, 0, sizeof(handler));

        handler.responseHandler.propertiesCallback =
            &ResponsePropertiesCallback;
        handler.responseHandler.completeCallback =
            &S3ReadStream::ResponseCompleteCallback;
        handler.getObjectDataCallback = &S3ReadStream::GetObjectDataCallback;

        // create request context
        S3Status status = S3_create_request_context(&req_ctx_);
        if (status != S3StatusOK)
            die("S3_create_request_context() failed.");

        // issue request but do not wait for data
        S3_get_object(bucket_context, key, getConditions, startByte, byteCount,
                      /* requestContext */ req_ctx_, &handler, this);
    }

    //! simpler constructor
    S3ReadStream(const S3BucketContext* bucket_context, const char* key,
                 uint64_t startByte = 0, uint64_t byteCount = 0)
        : S3ReadStream(bucket_context, key, /* getConditions */ nullptr,
                       startByte, byteCount) { }

    ~S3ReadStream() {
        close();
    }

    ssize_t read(void* data, size_t size) final {
        if (status_ != S3StatusOK)
            die("S3-ERROR during read: " << S3_get_status_name(status_));

        status_ = S3StatusOK;
        uint8_t* output_begin = reinterpret_cast<uint8_t*>(data);
        output_ = output_begin;
        output_end_ = output_ + size;

        // copy data from reception buffer
        size_t wb = std::min(size, buffer_.size());
        std::copy(buffer_.begin(), buffer_.begin() + wb, output_);
        output_ += wb;
        buffer_.erase(buffer_.begin(), buffer_.begin() + wb);

        // wait for more callbacks to deliver data, use select() to wait
        int remaining_requests = 1;
        while (status_ == S3StatusOK &&
               output_ < output_end_ && remaining_requests)
        {
            // perform a select() waiting on new data
            fd_set read_fds, write_fds, except_fds;
            FD_ZERO(&read_fds);
            FD_ZERO(&write_fds);
            FD_ZERO(&except_fds);
            int max_fd;

            S3Status status = S3_get_request_context_fdsets(
                req_ctx_, &read_fds, &write_fds, &except_fds, &max_fd);
            die_unless(status == S3StatusOK);

            if (max_fd != -1) {
                int r = select(max_fd + 1, &read_fds, &write_fds, &except_fds,
                               /* timeout */ nullptr);
                die_unless(r >= 0);
            }

            // run callbacks
            S3_runonce_request_context(req_ctx_, &remaining_requests);
        }

        return output_ - output_begin;
    }

    ssize_t lseek(off_t) final {
        die("Impossible.");
    }

    void close() final {
        if (!req_ctx_) return;

        S3_destroy_request_context(req_ctx_);
        req_ctx_ = nullptr;
    }

private:
    //! request context for waiting on more data
    S3RequestContext* req_ctx_;

    //! status of request
    S3Status status_ = S3StatusOK;

    //! reception buffer containing unneeded bytes from callback
    std::vector<uint8_t> buffer_;

    //! output buffer for read()
    uint8_t* output_;

    //! end of output buffer for read()
    uint8_t* output_end_;

    /**************************************************************************/

    //! completion callback, check for errors
    void ResponseCompleteCallback(
        S3Status status, const S3ErrorDetails* error) {
        status_ = status;

        if (status != S3StatusOK)
            LibS3LogError(status, error);
    }

    //! static wrapper to call
    static void ResponseCompleteCallback(
        S3Status status, const S3ErrorDetails* error, void* callbackData) {
        S3ReadStream* t = reinterpret_cast<S3ReadStream*>(callbackData);
        return t->ResponseCompleteCallback(status, error);
    }

    //! callback receiving data
    S3Status GetObjectDataCallback(int bufferSize, const char* buffer) {

        // copy as much data into output_ as fits
        size_t wb = std::min(
            output_end_ - output_, static_cast<intptr_t>(bufferSize));
        std::copy(buffer, buffer + wb, output_);
        output_ += wb;

        // store remaining unneeded bytes in buffer_
        if (wb != static_cast<uintptr_t>(bufferSize))
        {
            die_unless(output_ == output_end_);
            die_unless(buffer_.size() == 0);
            buffer_.resize(bufferSize - wb);
            std::copy(buffer + wb, buffer + bufferSize, buffer_.data());
        }

        return S3StatusOK;
    }

    //! static wrapper to call GetObjectDataCallback
    static S3Status GetObjectDataCallback(
        int bufferSize, const char* buffer, void* callbackData) {
        S3ReadStream* t = reinterpret_cast<S3ReadStream*>(callbackData);
        return t->GetObjectDataCallback(bufferSize, buffer);
    }
};

ReadStreamPtr S3OpenReadStream(
    const std::string& _path, const common::Range& range) {

    std::string path = _path;
    // crop off s3://
    die_unless(common::StartsWith(path, "s3://"));
    path = path.substr(5);

    // split uri into host/path
    std::vector<std::string> splitted = common::Split(path, '/', 2);

    // construct bucket
    S3BucketContext bkt;
    FillS3BucketContext(bkt, splitted[0]);

    // TODO(tb): figure out how to interpret range
    return common::MakeCounting<S3ReadStream>(
        &bkt, splitted[1].c_str(),
        /* startByte */ range.begin, /* byteCount */ range.size());
}

/******************************************************************************/

class S3WriteStream : public vfs::WriteStream
{
public:
    S3WriteStream(const std::string& bucket, const std::string& key,
                  S3PutProperties* putProperties = nullptr)
        : bucket_(bucket), key_(key),
          put_properties_(putProperties) {

        S3BucketContext bucket_context;
        FillS3BucketContext(bucket_context, bucket);

        // construct handlers
        S3MultipartInitialHandler handler;
        memset(&handler, 0, sizeof(handler));

        handler.responseHandler.propertiesCallback =
            &ResponsePropertiesCallback;
        handler.responseHandler.completeCallback =
            &S3WriteStream::ResponseCompleteCallback;
        handler.responseXmlCallback =
            &S3WriteStream::MultipartInitialResponseCallback;

        // create new multi part upload
        S3_initiate_multipart(
            &bucket_context, key_.c_str(), putProperties, &handler,
            /* requestContext */ nullptr, this);
    }

    ~S3WriteStream() {
        close();
    }

    ssize_t write(const void* _data, size_t size) final {
        const uint8_t* data = reinterpret_cast<const uint8_t*>(_data);

        while (size > 0)
        {
            // copy data to buffer
            size_t buffer_pos = buffer_.size();
            size_t wb = std::min(size, buffer_max_ - buffer_pos);
            buffer_.resize(buffer_pos + wb);
            std::copy(data, data + wb, buffer_.data() + buffer_pos);
            data += wb;
            size -= wb;

            if (buffer_.size() >= buffer_max_)
                UploadMultipart();
        }

        return size;
    }

    void close() final {
        if (!upload_id_.size()) return;

        // upload last multipart piece
        if (buffer_.size())
            UploadMultipart();

        LOG1 << "commit multipart";

        // construct commit XML

        std::ostringstream xml;
        xml << "<CompleteMultipartUpload>";
        for (size_t i = 0; i < part_etag_.size(); ++i) {
            xml << "<Part>"
                << "<PartNumber>" << (i + 1) << "</PartNumber>"
                << "<ETag>" << part_etag_[i] << "</ETag>"
                << "</Part>";
        }
        xml << "</CompleteMultipartUpload>";

        // put commit message into buffer_
        std::string xml_str = xml.str();
        upload_ = reinterpret_cast<const uint8_t*>(xml_str.data());
        upload_end_ = upload_ + xml_str.size();

        S3BucketContext bucket_context;
        FillS3BucketContext(bucket_context, bucket_);

        // construct handlers
        S3MultipartCommitHandler handler;
        memset(&handler, 0, sizeof(handler));

        handler.responseHandler.propertiesCallback =
            &ResponsePropertiesCallback;
        handler.responseHandler.completeCallback =
            &S3WriteStream::ResponseCompleteCallback;
        handler.putObjectDataCallback =
            &S3WriteStream::PutObjectDataCallback;
        handler.responseXmlCallback =
            &S3WriteStream::MultipartCommitResponseCallback;

        // synchronous upload of multi part data
        S3_complete_multipart_upload(
            &bucket_context, key_.c_str(), &handler, upload_id_.c_str(),
            /* contentLength */ xml_str.size(),
            /* requestContext */ nullptr, this);

        upload_id_.clear();
    }

private:
    //! status of request
    S3Status status_ = S3StatusOK;

    //! bucket for upload
    std::string bucket_;

    //! bucket key for upload
    std::string key_;

    //! put properties
    S3PutProperties* put_properties_;

    //! unique identifier for multi part upload
    std::string upload_id_;

    //! sequence number of uploads
    int upload_seq_ = 1;

    //! block size to upload as multi part
    size_t buffer_max_ = 16 * 1024 * 1024;

    //! output buffer, if this grows to 16 MiB a part upload is initiated.
    std::vector<uint8_t> buffer_;

    //! current upload position in buffer_ or other memory areas
    const uint8_t* upload_;

    //! end position of upload area
    const uint8_t* upload_end_;

    //! list of ETags of uploaded multiparts
    std::vector<std::string> part_etag_;

    /**************************************************************************/

    //! completion callback, check for errors
    void ResponseCompleteCallback(
        S3Status status, const S3ErrorDetails* error) {
        status_ = status;

        if (status != S3StatusOK)
            LibS3LogError(status, error);
    }

    //! static wrapper to call
    static void ResponseCompleteCallback(
        S3Status status, const S3ErrorDetails* error, void* callbackData) {
        S3WriteStream* t = reinterpret_cast<S3WriteStream*>(callbackData);
        return t->ResponseCompleteCallback(status, error);
    }

    //! initiation of multipart uploads
    static S3Status MultipartInitialResponseCallback(
        const char* upload_id, void* callbackData) {
        S3WriteStream* t = reinterpret_cast<S3WriteStream*>(callbackData);
        t->upload_id_ = upload_id;
        return S3StatusOK;
    }

    static S3Status MultipartCommitResponseCallback(
        const char* /* location */, const char* /* etag */,
        void* /* callbackData */) {
        // could save the parameters if we need them.
        return S3StatusOK;
    }

    /**************************************************************************/

    void UploadMultipart() {
        LOG1 << "S3-INFO - Upload multipart[" << upload_seq_ << "]"
             << " size " << buffer_.size();

        S3BucketContext bucket_context;
        FillS3BucketContext(bucket_context, bucket_);

        // construct handlers
        S3PutObjectHandler handler;
        memset(&handler, 0, sizeof(handler));

        handler.responseHandler.propertiesCallback =
            &S3WriteStream::MultipartPropertiesCallback;
        handler.responseHandler.completeCallback =
            &S3WriteStream::ResponseCompleteCallback;
        handler.putObjectDataCallback =
            &S3WriteStream::PutObjectDataCallback;

        // synchronous upload of multi part data
        upload_ = buffer_.data();
        upload_end_ = buffer_.data() + buffer_.size();
        S3_upload_part(&bucket_context, key_.c_str(), put_properties_,
                       &handler, upload_seq_++, upload_id_.c_str(),
                       /* partContentLength */ buffer_.size(),
                       /* requestContext */ nullptr, this);

        buffer_.clear();
    }

    S3Status MultipartPropertiesCallback(
        const S3ResponseProperties* properties) {

        part_etag_.emplace_back(properties->eTag);
        // output properties
        ResponsePropertiesCallback(properties, nullptr);
        return S3StatusOK;
    }

    static S3Status MultipartPropertiesCallback(
        const S3ResponseProperties* properties, void* callbackData) {
        S3WriteStream* t = reinterpret_cast<S3WriteStream*>(callbackData);
        return t->MultipartPropertiesCallback(properties);
    }

    int PutObjectDataCallback(int bufferSize, char* buffer) {
        size_t wb = std::min(
            static_cast<intptr_t>(bufferSize), upload_end_ - upload_);
        std::copy(upload_, upload_ + wb, buffer);
        upload_ += wb;
        return wb;
    }

    static int PutObjectDataCallback(
        int bufferSize, char* buffer, void* callbackData) {
        S3WriteStream* t = reinterpret_cast<S3WriteStream*>(callbackData);
        return t->PutObjectDataCallback(bufferSize, buffer);
    }
};

WriteStreamPtr S3OpenWriteStream(const std::string& _path) {

    std::string path = _path;
    // crop off s3://
    die_unless(common::StartsWith(path, "s3://"));
    path = path.substr(5);

    // split uri into host/path
    std::vector<std::string> splitted = common::Split(path, '/', 2);

    return common::MakeCounting<S3WriteStream>(splitted[0], splitted[1]);
}

#else   // !THRILL_HAVE_LIBS3

void S3Initialize()
{ }

void S3Deinitialize()
{ }

ReadStreamPtr S3OpenReadStream(
    const std::string& /* path */, const common::Range& /* range */) {
    die("s3:// is not available, because Thrill was built without libS3.");
}

WriteStreamPtr S3OpenWriteStream(const std::string& /* path */) {
    die("s3:// is not available, because Thrill was built without libS3.");
}

#endif  // !THRILL_HAVE_LIBS3

} // namespace vfs
} // namespace thrill

/******************************************************************************/
