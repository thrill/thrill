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
                  bool distributed_fs)
        : Super(ctx, "ReadLines"),
          distributed_fs_(distributed_fs) {

        LOG << "Opening ReadLinesNode for " << globlist.size() << " globs";

        filelist_ = vfs::GlobFileSizePrefixSum(vfs::GlobFilePatterns(globlist));

        if (filelist_.count() == 0) {
            throw std::runtime_error(
                      "No files found in globs: "
                      + common::Join(" ", globlist));
        }
    }

    //! Constructor for a ReadLinesNode. Sets the Context and file path.
    ReadLinesNode(Context& ctx, const std::string& glob, bool distributed_fs)
        : ReadLinesNode(ctx, std::vector<std::string>{ glob }, distributed_fs)
    { }

    DIAMemUse PushDataMemUse() final {
        // InputLineIterators read files block-wise
        return data::default_block_size;
    }

    void PushData(bool /* consume */) final {
        if (filelist_.contains_compressed) {
            InputLineIteratorCompressed it = InputLineIteratorCompressed(
                filelist_, context_, *this, distributed_fs_);

            // Hook Read
            while (it.HasNext()) {
                this->PushItem(it.Next());
            }
        }
        else {
            InputLineIteratorUncompressed it = InputLineIteratorUncompressed(
                filelist_, context_, *this, distributed_fs_);

            // Hook Read
            while (it.HasNext()) {
                this->PushItem(it.Next());//it.Next());
            }
        }
    }

private:
    vfs::FileList filelist_;

    //! True, if files are on a distributed file system
    bool distributed_fs_;

    class InputLineIterator
    {
    public:
        InputLineIterator(const vfs::FileList& files,
                          const Context& context,
                          ReadLinesNode& node)
            : files_(files), context_(context), node_(node) { }

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

        const Context& context_;
        //! Index of current file in files_
        size_t current_file_ = 0;
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
            LOG << "Opening block with " << bytes << " bytes.";
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
                                      const api::Context& ctx,
                                      ReadLinesNode& node, bool distributed_fs)
            : InputLineIterator(files, ctx, node) {

            // Go to start of 'local part'.
            if (distributed_fs) {
                my_range_ = node_.context_.CalculateLocalRange(
                    files.total_size);
            }
            else {
                my_range_ = node_.context_.CalculateLocalRangeOnHost(
                    files.total_size);
            }

            while (files_.list[current_file_].size_inc_psum() <= my_range_.begin) {
                current_file_++;
            }
            if (my_range_.begin < my_range_.end) {
                sLOG << "Opening file" << current_file_
                     << "my_range_" << my_range_;

                stream_ = vfs::OpenReadStream(
                    files_.list[current_file_].path, my_range_);
            }
            else {
                LOG << "my_range : " << my_range_;
                return;
            }

            // find offset in current file:
            // offset = start - sum of previous file sizes
            offset_ = stream_->lseek(
                static_cast<off_t>(my_range_.begin - files_.list[current_file_].size_ex_psum));
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
                while (current_ < buffer_.end()) {
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
                    LOG << "opening next file";

                    stream_->close();
                    current_file_++;
                    offset_ = 0;

                    if (current_file_ < files_.count()) {
                        stream_ = vfs::OpenReadStream(
                            files_.list[current_file_].path, my_range_);
                        offset_ += buffer_.size();
                        ReadBlock(stream_, buffer_);
                    }
                    else {
                        current_ = buffer_.begin() +
                                   files_.list[current_file_ - 1].size;
                    }

                    if (data_.length()) {
                        return data_;
                    }
                }
            }
        }

        //! returns true, if an element is available in local part
        bool HasNext() {
            size_t position_in_buf = current_ - buffer_.begin();
            assert(current_ >= buffer_.begin());
            size_t global_index = offset_ + position_in_buf + files_.list[current_file_].size_ex_psum;
            return global_index < my_range_.end ||
                   (global_index == my_range_.end &&
                    files_.list[current_file_].size > offset_ + position_in_buf);
        }

    private:
        //! Offset of current block in stream_.
        size_t offset_ = 0;
        //! File handle to files_[current_file_]
        vfs::ReadStreamPtr stream_;
    };

    //! InputLineIterator gives you access to lines of a file
    class InputLineIteratorCompressed : public InputLineIterator
    {
    public:
        //! Creates an instance of iterator that reads file line based
        InputLineIteratorCompressed(const vfs::FileList& files,
                                    const api::Context& ctx,
                                    ReadLinesNode& node, bool distributed_fs)
            : InputLineIterator(files, ctx, node) {

            // Go to start of 'local part'.
            if (distributed_fs) {
                my_range_ = node_.context_.CalculateLocalRange(
                    files.total_size);
            }
            else {
                my_range_ = node_.context_.CalculateLocalRangeOnHost(
                    files.total_size);
            }

            while (files_.list[current_file_].size_inc_psum() <= my_range_.begin) {
                current_file_++;
            }

            for (size_t file_nr = current_file_; file_nr < files_.count(); file_nr++) {
                if (files.list[file_nr].size_inc_psum() == my_range_.end) {
                    break;
                }
                if (files.list[file_nr].size_inc_psum() > my_range_.end) {
                    my_range_.end = files_.list[file_nr].size_ex_psum;
                    break;
                }
            }

            if (my_range_.begin < my_range_.end) {
                LOG << "Opening file " << current_file_;
                LOG << "my_range : " << my_range_;
                stream_ = vfs::OpenReadStream(
                    files_.list[current_file_].path, my_range_);
            }
            else {
                // No local files, set buffer size to 2, so HasNext() does not try to read
                LOG << "my_range : " << my_range_;
                buffer_.Reserve(2);
                buffer_.set_size(2);
                current_ = buffer_.begin();
                return;
            }
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
                    LOG << "Opening new file!";
                    stream_->close();
                    current_file_++;

                    if (current_file_ < files_.count()) {
                        stream_ = vfs::OpenReadStream(
                            files_.list[current_file_].path, my_range_);
                        ReadBlock(stream_, buffer_);
                    }
                    else {
                        LOG << "reached last file";
                        current_ = buffer_.begin();
                    }

                    if (data_.length()) {
                        LOG << "end - returning string of length" << data_.length();
                        return data_;
                    }
                }
            }
        }

        //! returns true, if an element is available in local part
        bool HasNext() {
            if (files_.list[current_file_].size_ex_psum >= my_range_.end) {
                return false;
            }

            // if block is fully read, read next block. needs to be done here
            // as HasNext() has to know if file is finished
            //         v-- no new line at end ||   v-- newline at end of file
            if (current_ >= buffer_.end() || (current_ + 1 >= buffer_.end() && *current_ == '\n')) {
                LOG << "New buffer in HasNext()";
                ReadBlock(stream_, buffer_);
                if (buffer_.size() > 1 || (buffer_.size() == 1 && buffer_[0] != '\n')) {
                    return true;
                }
                else {
                    LOG << "Opening new file in HasNext()";
                    // already at last file
                    if (current_file_ >= files_.count() - 1) {
                        return false;
                    }
                    stream_->close();
                    // if (this worker reads at least one more file)
                    if (my_range_.end > files_.list[current_file_].size_inc_psum()) {
                        current_file_++;
                        stream_ = vfs::OpenReadStream(
                            files_.list[current_file_].path, my_range_);
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
        //! File handle to files_[current_file_]
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
        common::MakeCounting<ReadLinesNode>(ctx, filepath,
                                            /* distributed FS */ true));
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
        common::MakeCounting<ReadLinesNode>(ctx, filepaths,
                                            /* distributed FS */ true));
}

DIA<std::string> ReadLines(struct LocalStorageTag, Context& ctx,
                           const std::string& filepath) {
    return DIA<std::string>(
        common::MakeCounting<ReadLinesNode>(ctx, filepath,
                                            /* distributed FS */ false));
}

DIA<std::string> ReadLines(struct LocalStorageTag, Context& ctx,
                           const std::vector<std::string>& filepaths) {
    return DIA<std::string>(
        common::MakeCounting<ReadLinesNode>(ctx, filepaths,
                                            /* distributed FS */ false));
}

} // namespace api

//! imported from api namespace
using api::ReadLines;

} // namespace thrill

#endif // !THRILL_API_READ_LINES_HEADER

/******************************************************************************/
