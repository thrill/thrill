/*******************************************************************************
 * thrill/vfs/bzip2_filter.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/bzip2_filter.hpp>

#include <thrill/common/die.hpp>

#if THRILL_HAVE_BZIP2
#include <bzlib.h>
#endif

#include <vector>

namespace thrill {
namespace vfs {

#if THRILL_HAVE_BZIP2

/******************************************************************************/
// BZip2WriteFilter - on-the-fly bzip2 compressor

class BZip2WriteFilter final : public virtual WriteStream
{
public:
    explicit BZip2WriteFilter(const WriteStreamPtr& output)
        : output_(output) {
        memset(&bz_stream_, 0, sizeof(bz_stream_));

        int err = BZ2_bzCompressInit(
            &bz_stream_,
            /* blockSize100k */ 9, /* verbosity */ 0, /* workFactor */ 0);
        die_unequal(err, BZ_OK);

        // output buffer
        buffer_.resize(2 * 1024 * 1024);
        bz_stream_.next_out = buffer_.data();
        bz_stream_.avail_out = static_cast<unsigned>(buffer_.size());

        initialized_ = true;
    }

    ~BZip2WriteFilter() {
        close();
    }

    ssize_t write(const void* data, const size_t size) final {
        int err;

        bz_stream_.next_in = const_cast<char*>(
            reinterpret_cast<const char*>(data));
        bz_stream_.avail_in = size;

        do
        {
            err = BZ2_bzCompress(&bz_stream_, BZ_RUN);

            if (err == BZ_OK && bz_stream_.avail_in != 0)
            {
                unsigned written_size =
                    buffer_.size() - bz_stream_.avail_out;

                // buffer is full, write to output
                output_->write(buffer_.data(), written_size);

                bz_stream_.next_out = buffer_.data();
                bz_stream_.avail_out = buffer_.size();
            }
        }
        while (bz_stream_.avail_in != 0 && err == BZ_RUN_OK); // NOLINT

        die_unequal(err, BZ_RUN_OK);

        return size;
    }

    void close() final {
        if (!initialized_) return;

        int err;

        do
        {
            err = BZ2_bzCompress(&bz_stream_, BZ_FINISH);

            if (err == BZ_FINISH_OK && bz_stream_.avail_in != 0)
            {
                unsigned written_size =
                    buffer_.size() - bz_stream_.avail_out;

                // buffer is full, write to output
                output_->write(buffer_.data(), written_size);

                bz_stream_.next_out = buffer_.data();
                bz_stream_.avail_out = buffer_.size();
            }
        }
        while (err == BZ_FINISH_OK); // NOLINT

        die_unequal(err, BZ_STREAM_END);

        // write remaining data
        unsigned written_size = buffer_.size() - bz_stream_.avail_out;
        output_->write(buffer_.data(), written_size);

        output_->close();

        BZ2_bzCompressEnd(&bz_stream_);
        initialized_ = false;
    }

private:
    //! if bz_stream_ is initialized
    bool initialized_;

    //! bzip2 context
    bz_stream bz_stream_;

    //! compression buffer, flushed to output when full
    std::vector<char> buffer_;

    //! output stream for writing data somewhere
    WriteStreamPtr output_;
};

WriteStreamPtr MakeBZip2WriteFilter(const WriteStreamPtr& stream) {
    die_unless(stream);
    return common::MakeCounting<BZip2WriteFilter>(stream);
}

/******************************************************************************/
// BZip2ReadFilter - on-the-fly bzip2 decompressor

class BZip2ReadFilter : public virtual ReadStream
{
public:
    explicit BZip2ReadFilter(const ReadStreamPtr& input)
        : input_(input) {
        memset(&bz_stream_, 0, sizeof(bz_stream_));

        err_ = BZ2_bzDecompressInit(
            &bz_stream_, /* verbosity */ 0, /* small */ 0);
        die_unequal(err_, BZ_OK);

        // output buffer
        buffer_.resize(2 * 1024 * 1024);
        bz_stream_.next_in = buffer_.data();
        bz_stream_.avail_in = 0;

        initialized_ = true;
    }

    ~BZip2ReadFilter() {
        close();
    }

    ssize_t read(void* data, size_t size) final {
        bz_stream_.next_out = const_cast<char*>(
            reinterpret_cast<const char*>(data));
        bz_stream_.avail_out = size;

        do
        {
            if (bz_stream_.avail_in == 0) {
                // input buffer empty, so read from input_
                bz_stream_.avail_in = input_->read(
                    buffer_.data(), buffer_.size());
                bz_stream_.next_in = buffer_.data();

                if (bz_stream_.avail_in == 0) {
                    return size - bz_stream_.avail_out;
                }
            }

            err_ = BZ2_bzDecompress(&bz_stream_);

            if (err_ == BZ_STREAM_END)
                return size - bz_stream_.avail_out;
        }
        while (err_ == BZ_OK && bz_stream_.avail_out != 0);  // NOLINT

        die_unequal(bz_stream_.avail_out, 0u);

        return size;
    }

    void close() final {
        if (!initialized_) return;

        BZ2_bzDecompressEnd(&bz_stream_);
        input_->close();

        initialized_ = false;
    }

private:
    //! if bz_stream_ is initialized
    bool initialized_;

    //! bzip2 context
    bz_stream bz_stream_;

    //! current error code
    int err_;

    //! decompression buffer, filled from the input when empty
    std::vector<char> buffer_;

    //! input stream for reading data from somewhere
    ReadStreamPtr input_;
};

ReadStreamPtr MakeBZip2ReadFilter(const ReadStreamPtr& stream) {
    die_unless(stream);
    return common::MakeCounting<BZip2ReadFilter>(stream);
}

/******************************************************************************/

#else   // !THRILL_HAVE_BZIP2

WriteStreamPtr MakeBZip2WriteFilter(const WriteStreamPtr&) {
    die(".bz2 decompression is not available, "
        "because Thrill was built without libbz2.");
}

ReadStreamPtr MakeBZip2ReadFilter(const ReadStreamPtr&) {
    die(".bz2 decompression is not available, "
        "because Thrill was built without libbz2.");
}

#endif

} // namespace vfs
} // namespace thrill

/******************************************************************************/
