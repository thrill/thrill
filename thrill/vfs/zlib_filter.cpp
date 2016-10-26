/*******************************************************************************
 * thrill/vfs/zlib_filter.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/zlib_filter.hpp>

#include <thrill/common/die.hpp>

#include <zlib.h>

#include <vector>

namespace thrill {
namespace vfs {

/******************************************************************************/
// ZLibWriteFilter - on-the-fly zlib compressor

class ZLibWriteFilter final : public virtual WriteStream
{
public:
    explicit ZLibWriteFilter(const WriteStreamPtr& output)
        : output_(output) {
        memset(&z_stream_, 0, sizeof(z_stream_));

        // windowBits = 15 (largest allocation) + 16 (gzip header)
        int window_size = 15 + 16;
        int err = deflateInit2(&z_stream_, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                               window_size, /* memLevel */ 8,
                               Z_DEFAULT_STRATEGY);
        die_unequal(err, Z_OK);

        // output buffer
        buffer_.resize(2 * 1024 * 1024);
        z_stream_.next_out = buffer_.data();
        z_stream_.avail_out = static_cast<uInt>(buffer_.size());

        initialized_ = true;
    }

    ~ZLibWriteFilter() {
        close();
    }

    ssize_t write(const void* data, const size_t size) final {
        int err;

        z_stream_.next_in = const_cast<Bytef*>(
            reinterpret_cast<const Bytef*>(data));
        z_stream_.avail_in = size;

        do
        {
            err = deflate(&z_stream_, Z_NO_FLUSH);

            if (err == Z_OK && z_stream_.avail_in != 0)
            {
                uInt written_size =
                    buffer_.size() - z_stream_.avail_out;

                // buffer is full, write to output
                output_->write(buffer_.data(), written_size);

                z_stream_.next_out = buffer_.data();
                z_stream_.avail_out = buffer_.size();
            }
        }
        while (z_stream_.avail_in != 0 && err == Z_OK); // NOLINT

        die_unequal(err, Z_OK);

        return size;
    }

    void close() final {
        if (!initialized_) return;

        int err;

        do
        {
            err = deflate(&z_stream_, Z_FINISH);

            if (err == Z_OK && z_stream_.avail_in != 0)
            {
                uInt written_size =
                    buffer_.size() - z_stream_.avail_out;

                // buffer is full, write to output
                output_->write(buffer_.data(), written_size);

                z_stream_.next_out = buffer_.data();
                z_stream_.avail_out = buffer_.size();
            }
        }
        while (err == Z_OK); // NOLINT

        // write remaining data
        uInt written_size = buffer_.size() - z_stream_.avail_out;
        output_->write(buffer_.data(), written_size);

        output_->close();

        deflateEnd(&z_stream_);
        initialized_ = false;
    }

private:
    //! if z_stream_ is initialized
    bool initialized_;

    //! zlib context
    z_stream z_stream_;

    //! compression buffer, flushed to output when full
    std::vector<Bytef> buffer_;

    //! output stream for writing data somewhere
    WriteStreamPtr output_;
};

WriteStreamPtr MakeZLibWriteFilter(const WriteStreamPtr& stream) {
    die_unless(stream);
    return common::MakeCounting<ZLibWriteFilter>(stream);
}

/******************************************************************************/
// ZLibReadFilter - on-the-fly zlib decompressor

class ZLibReadFilter : public virtual ReadStream
{
public:
    explicit ZLibReadFilter(const ReadStreamPtr& input)
        : input_(input) {
        memset(&z_stream_, 0, sizeof(z_stream_));

        /* windowBits = 15 (largest allocation) + 32 (autodetect headers) */
        int window_size = 15 + 32;
        err_ = inflateInit2(&z_stream_, window_size);
        die_unequal(err_, Z_OK);

        // output buffer
        buffer_.resize(2 * 1024 * 1024);
        z_stream_.next_in = buffer_.data();
        z_stream_.avail_in = 0;

        initialized_ = true;
    }

    ~ZLibReadFilter() {
        close();
    }

    ssize_t read(void* data, size_t size) final {
        z_stream_.next_out = const_cast<Bytef*>(
            reinterpret_cast<const Bytef*>(data));
        z_stream_.avail_out = size;

        do
        {
            if (z_stream_.avail_in == 0) {
                // input buffer empty, so read from input_
                z_stream_.avail_in = input_->read(
                    buffer_.data(), buffer_.size());
                z_stream_.next_in = buffer_.data();

                if (z_stream_.avail_in == 0) {
                    return size - z_stream_.avail_out;
                }
            }

            if (err_ == Z_STREAM_END) {
                LOG1 << "inflateReset";
                inflateReset(&z_stream_);
            }

            err_ = inflate(&z_stream_, Z_SYNC_FLUSH);
        }
        while ((err_ == Z_OK || err_ == Z_STREAM_END) && // NOLINT
               z_stream_.avail_out != 0);                // NOLINT

        die_unequal(z_stream_.avail_out, 0u);

        return size;
    }

    ssize_t lseek(off_t) final {
        die("impossible");
    }

    void close() final {
        if (!initialized_) return;

        inflateEnd(&z_stream_);
        input_->close();

        initialized_ = false;
    }

private:
    //! if z_stream_ is initialized
    bool initialized_;

    //! zlib context
    z_stream z_stream_;

    //! current error code
    int err_;

    //! decompression buffer, filled from the input when empty
    std::vector<Bytef> buffer_;

    //! input stream for reading data from somewhere
    ReadStreamPtr input_;
};

ReadStreamPtr MakeZLibReadFilter(const ReadStreamPtr& stream) {
    die_unless(stream);
    return common::MakeCounting<ZLibReadFilter>(stream);
}

/******************************************************************************/

} // namespace vfs
} // namespace thrill

/******************************************************************************/
