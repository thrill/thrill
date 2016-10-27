/*******************************************************************************
 * thrill/api/read_lines.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_READ_LINES_HEADER
#define THRILL_API_READ_LINES_HEADER

#include <thrill/api/context.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/source_node.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/net/buffer_builder.hpp>
#include <thrill/vfs/file_io.hpp>

#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

/*!
 * A DIANode which performs a line-based Read operation. Reads a file from the
 * file system and delivers it as a DIA.
 *
 * \ingroup api_layer
 */
class ReadLinesNode final : public SourceNode<std::string>
{
    static constexpr bool debug = false;

public:
    using Super = SourceNode<std::string>;
    using Super::context_;

    //! Constructor for a ReadLinesNode. Sets the Context and file path.
    ReadLinesNode(Context& ctx, const std::vector<std::string>& globlist,
                  bool local_storage)
        : Super(ctx, "ReadLines"),
          local_storage_(local_storage) {

        filelist_ = vfs::Glob(globlist);

        if (filelist_.size() == 0)
            die("ReadLines: no files found in globs: " + common::Join(" ", globlist));

        sLOG << "ReadLines: creating for" << globlist.size() << "globs"
             << "matching" << filelist_.size() << "files";
    }

    //! Constructor for a ReadLinesNode. Sets the Context and file path.
    ReadLinesNode(Context& ctx, const std::string& glob, bool local_storage)
        : ReadLinesNode(ctx, std::vector<std::string>{ glob }, local_storage)
    { }

    DIAMemUse PushDataMemUse() final {
        // InputLineIterators read files block-wise
        return data::default_block_size;
    }

    void PushData(bool /* consume */) final {
        if (filelist_.contains_compressed) {
            InputLineIteratorCompressed it(
                filelist_, *this, local_storage_);

            // Hook Read
            while (it.HasNext()) {
                this->PushItem(it.Next());
            }
        }
        else {
            InputLineIteratorUncompressed it(
                filelist_, *this, local_storage_);

            // Hook Read
            while (it.HasNext()) {
                this->PushItem(it.Next());//it.Next());
            }
        }
    }

private:
    vfs::FileList filelist_;

    //! true, if files are on a local file system, false: common global file
    //! system.
    bool local_storage_;

    class InputLineIterator
    {
    public:
        InputLineIterator(const vfs::FileList& files,
                          ReadLinesNode& node)
            : files_(files), node_(node) { }

        //! non-copyable: delete copy-constructor
        InputLineIterator(const InputLineIterator&) = delete;
        //! non-copyable: delete assignment operator
        InputLineIterator& operator = (const InputLineIterator&) = delete;
        //! move-constructor: default
        InputLineIterator(InputLineIterator&&) = default;
        //! move-assignment operator: default
        InputLineIterator& operator = (InputLineIterator&&) = default;

    protected:
        //! Block read size
        const size_t read_size = data::default_block_size;
        //! String, which Next() references to
        std::string data_;
        //! Input files with size prefixsum.
        const vfs::FileList& files_;

        //! Index of current file in files_
        size_t file_nr_ = 0;
        //! Byte buffer to create line std::string values.
        net::BufferBuilder buffer_;
        //! Start of next element in current buffer.
        unsigned char* current_;
        //! (exclusive) [begin,end) of local block
        common::Range my_range_;
        //! Reference to node
        ReadLinesNode& node_;

        common::StatsTimerStopped read_timer;

        size_t total_bytes_ = 0;
        size_t total_reads_ = 0;
        size_t total_elements_ = 0;

        bool ReadBlock(vfs::ReadStreamPtr& file,
                       net::BufferBuilder& buffer) {
            read_timer.Start();
            ssize_t bytes = file->read(buffer.data(), read_size);
            read_timer.Stop();
            if (bytes < 0) {
                throw common::ErrnoException("Read error");
            }
            buffer.set_size(bytes);
            current_ = buffer.begin();
            total_bytes_ += bytes;
            total_reads_++;
            LOG << "ReadLines: read block containing " << bytes << " bytes.";
            return bytes > 0;
        }

        ~InputLineIterator() {
            node_.logger_
                << "class" << "ReadLinesNode"
                << "event" << "done"
                << "total_bytes" << total_bytes_
                << "total_reads" << total_reads_
                << "total_lines" << total_elements_
                << "read_time" << read_timer.Milliseconds();
        }
    };

    //! InputLineIterator gives you access to lines of a file
    class InputLineIteratorUncompressed : public InputLineIterator
    {
    public:
        //! Creates an instance of iterator that reads file line based
        InputLineIteratorUncompressed(const vfs::FileList& files,
                                      ReadLinesNode& node, bool local_storage)
            : InputLineIterator(files, node) {

            // Go to start of 'local part'.
            if (local_storage) {
                my_range_ = node_.context_.CalculateLocalRangeOnHost(
                    files.total_size);
            }
            else {
                my_range_ = node_.context_.CalculateLocalRange(
                    files.total_size);
            }

            assert(my_range_.begin <= my_range_.end);
            if (my_range_.begin == my_range_.end) return;

            while (files_[file_nr_].size_inc_psum() <= my_range_.begin) {
                file_nr_++;
            }

            offset_ = my_range_.begin - files_.size_ex_psum(file_nr_);

            sLOG << "ReadLines: opening file" << file_nr_
                 << "my_range_" << my_range_ << "offset_" << offset_;

            // find offset in current file:
            // offset = start - sum of previous file sizes
            stream_ = vfs::OpenReadStream(
                files_[file_nr_].path, common::Range(offset_, 0));

            buffer_.Reserve(read_size);
            ReadBlock(stream_, buffer_);

            if (offset_ != 0) {
                bool found_n = false;

                // find next newline, discard all previous data as previous
                // worker already covers it
                while (!found_n) {
                    while (current_ < buffer_.end()) {
                        if (THRILL_UNLIKELY(*current_++ == '\n')) {
                            found_n = true;
                            break;
                        }
                    }
                    // no newline found: read new data into buffer_builder
                    if (!found_n) {
                        offset_ += buffer_.size();
                        if (!ReadBlock(stream_, buffer_)) {
                            // EOF = newline per definition
                            found_n = true;
                        }
                    }
                }
            }
            data_.reserve(4 * 1024);
        }

        //! returns the next element if one exists
        //!
        //! does no checks whether a next element exists!
        const std::string& Next() {
            total_elements_++;
            data_.clear();
            while (true) {
                while (THRILL_LIKELY(current_ < buffer_.end())) {
                    if (THRILL_UNLIKELY(*current_ == '\n')) {
                        current_++;
                        return data_;
                    }
                    else {
                        data_.push_back(*current_++);
                    }
                }
                offset_ += buffer_.size();
                if (!ReadBlock(stream_, buffer_)) {
                    LOG << "ReadLines: opening next file";

                    stream_->close();
                    file_nr_++;
                    offset_ = 0;

                    if (file_nr_ < files_.size()) {
                        stream_ = vfs::OpenReadStream(
                            files_[file_nr_].path, common::Range(offset_, 0));
                        offset_ += buffer_.size();
                        ReadBlock(stream_, buffer_);
                    }
                    else {
                        current_ = buffer_.begin() +
                                   files_[file_nr_ - 1].size;
                    }

                    if (data_.length()) {
                        return data_;
                    }
                }
            }
        }

        //! returns true, if an element is available in local part
        bool HasNext() {
            size_t pos = current_ - buffer_.begin();
            assert(current_ >= buffer_.begin());
            size_t global_index = offset_ + pos + files_.size_ex_psum(file_nr_);
            return global_index < my_range_.end ||
                   (global_index == my_range_.end &&
                    files_[file_nr_].size > offset_ + pos);
        }

    private:
        //! Offset of current block in stream_.
        size_t offset_ = 0;
        //! File handle to files_[file_nr_]
        vfs::ReadStreamPtr stream_;
    };

    //! InputLineIterator gives you access to lines of a file
    class InputLineIteratorCompressed : public InputLineIterator
    {
    public:
        //! Creates an instance of iterator that reads file line based
        InputLineIteratorCompressed(const vfs::FileList& files,
                                    ReadLinesNode& node, bool local_storage)
            : InputLineIterator(files, node) {

            // Go to start of 'local part'.
            if (local_storage) {
                my_range_ = node_.context_.CalculateLocalRangeOnHost(
                    files.total_size);
            }
            else {
                my_range_ = node_.context_.CalculateLocalRange(
                    files.total_size);
            }

            while (files_[file_nr_].size_inc_psum() <= my_range_.begin) {
                file_nr_++;
            }

            for (size_t i = file_nr_; i < files_.size(); i++) {
                if (files[i].size_inc_psum() == my_range_.end) {
                    break;
                }
                if (files[i].size_inc_psum() > my_range_.end) {
                    my_range_.end = files_.size_ex_psum(i);
                    break;
                }
            }

            if (my_range_.begin >= my_range_.end) {
                // No local files, set buffer size to 2, so HasNext() does not try to read
                LOG << "ReadLines: my_range " << my_range_;
                buffer_.Reserve(2);
                buffer_.set_size(2);
                current_ = buffer_.begin();
                return;
            }

            sLOG << "ReadLines: opening compressed file" << file_nr_
                 << "my_range" << my_range_;

            stream_ = vfs::OpenReadStream(files_[file_nr_].path);

            buffer_.Reserve(read_size);
            ReadBlock(stream_, buffer_);
            data_.reserve(4 * 1024);
        }

        //! returns the next element if one exists
        //!
        //! does no checks whether a next element exists!
        const std::string& Next() {
            total_elements_++;
            data_.clear();
            while (true) {
                while (current_ < buffer_.end()) {
                    if (THRILL_UNLIKELY(*current_ == '\n')) {
                        current_++;
                        return data_;
                    }
                    else {
                        data_.push_back(*current_++);
                    }
                }

                if (!ReadBlock(stream_, buffer_)) {
                    LOG << "ReadLines: opening new compressed file!";
                    stream_->close();
                    file_nr_++;

                    if (file_nr_ < files_.size()) {
                        stream_ = vfs::OpenReadStream(files_[file_nr_].path);
                        ReadBlock(stream_, buffer_);
                    }
                    else {
                        LOG << "ReadLines: reached last file";
                        current_ = buffer_.begin();
                    }

                    if (data_.length()) {
                        LOG << "ReadLines: end - returning string of length"
                            << data_.length();
                        return data_;
                    }
                }
            }
        }

        //! returns true, if an element is available in local part
        bool HasNext() {
            if (files_.size_ex_psum(file_nr_) >= my_range_.end) {
                return false;
            }

            // if block is fully read, read next block. needs to be done here
            // as HasNext() has to know if file is finished
            //         v-- no new line at end ||   v-- newline at end of file
            if (current_ >= buffer_.end() || (current_ + 1 >= buffer_.end() && *current_ == '\n')) {
                LOG << "ReadLines: new buffer in HasNext()";
                ReadBlock(stream_, buffer_);
                if (buffer_.size() > 1 || (buffer_.size() == 1 && buffer_[0] != '\n')) {
                    return true;
                }
                else {
                    LOG << "ReadLines: opening new file in HasNext()";
                    // already at last file
                    if (file_nr_ >= files_.size() - 1) {
                        return false;
                    }
                    stream_->close();
                    // if (this worker reads at least one more file)
                    if (my_range_.end > files_[file_nr_].size_inc_psum()) {
                        file_nr_++;
                        stream_ = vfs::OpenReadStream(files_[file_nr_].path);
                        ReadBlock(stream_, buffer_);
                        return true;
                    }
                    else {
                        return false;
                    }
                }
            }
            else {
                return true;
            }
        }

    private:
        //! File handle to files_[file_nr_]
        vfs::ReadStreamPtr stream_;
    };
};

/*!
 * ReadLines is a DOp, which reads a file from the file system and
 * creates an ordered DIA according to a given read function.
 *
 * \param ctx Reference to the context object
 * \param filepath Path of the file in the file system
 *
 * \ingroup dia_sources
 */
DIA<std::string> ReadLines(Context& ctx, const std::string& filepath) {
    return DIA<std::string>(
        common::MakeCounting<ReadLinesNode>(
            ctx, filepath, /* local_storage */ false));
}

/*!
 * ReadLines is a DOp, which reads a file from the file system and
 * creates an ordered DIA according to a given read function.
 *
 * \param ctx Reference to the context object
 * \param filepaths Path of the file in the file system
 *
 * \ingroup dia_sources
 */
DIA<std::string> ReadLines(
    Context& ctx, const std::vector<std::string>& filepaths) {
    return DIA<std::string>(
        common::MakeCounting<ReadLinesNode>(
            ctx, filepaths, /* local_storage */ false));
}

DIA<std::string> ReadLines(struct LocalStorageTag, Context& ctx,
                           const std::string& filepath) {
    return DIA<std::string>(
        common::MakeCounting<ReadLinesNode>(
            ctx, filepath, /* local_storage */ true));
}

DIA<std::string> ReadLines(struct LocalStorageTag, Context& ctx,
                           const std::vector<std::string>& filepaths) {
    return DIA<std::string>(
        common::MakeCounting<ReadLinesNode>(
            ctx, filepaths, /* local_storage */ true));
}

} // namespace api

//! imported from api namespace
using api::ReadLines;

} // namespace thrill

#endif // !THRILL_API_READ_LINES_HEADER

/******************************************************************************/
