/*******************************************************************************
 * thrill/api/read_lines.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_READ_LINES_HEADER
#define THRILL_API_READ_LINES_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/source_node.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <thrill/common/string.hpp>
#include <thrill/core/file_io.hpp>
#include <thrill/net/buffer_builder.hpp>

#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a line-based Read operation. Read reads a file from
 * the file system and emits it as a DIA.
 */
class ReadLinesNode : public SourceNode<std::string>
{
public:
    using Super = SourceNode<std::string>;
    using Super::context_;

    using FileSizePair = std::pair<std::string, size_t>;

    static const bool debug = false;

    /*!
     * Constructor for a ReadLinesNode. Sets the Context
     * and file path.
     *
     * \param ctx Reference to Context, which holds references to data and
     * network.
     *
     * \param path Path of the input file(s)
     */
    ReadLinesNode(Context& ctx,
                  const std::string& path,
                  StatsNode* stats_node)
        : Super(ctx, { }, stats_node),
          path_(path)
    {
		LOG << "Opening read notes for " << path_;
		
        filesize_prefix_ = core::GlobFileSizePrefixSum(path_);

        for (auto file : filesize_prefix_) {
            if (core::IsCompressed(file.first)) {
                contains_compressed_file_ = true;
                break;
            }
        }
    }

    void PushData() final {
        if (contains_compressed_file_) {
            InputLineIteratorCompressed it = InputLineIteratorCompressed(
                filesize_prefix_, context_.my_rank(), context_.num_workers());

            // Hook Read
            while (it.HasNext()) {
                this->PushItem(it.Next());
            }
        }
        else {
            InputLineIteratorUncompressed it = InputLineIteratorUncompressed(
                filesize_prefix_, context_.my_rank(), context_.num_workers());

            // Hook Read
            while (it.HasNext()) {
                this->PushItem(it.Next());
            }
        }
    }

    void Dispose() final { }

    /*!
     * Produces an 'empty' function stack, which only contains the identity
     * emitter function.
     *
     * \return Empty function stack
     */
    auto ProduceStack() {
        return FunctionStack<std::string>();
    }

private:
    //! True, if at least one input file is compressed.
    bool contains_compressed_file_ = false;
    //! Path of the input file.
    std::string path_;

    std::vector<std::pair<std::string, size_t> > filesize_prefix_;

    // REVIEW(an): this is useless, you never use the inheritance.  But, you
    // actually SHOULD use it! for all member fields and methods that are in
    // common. But NOT for virtual functions. Remove the virtuals. Find out what
    // functions the methods below have in common and make them functions of the
    // superclass.
    class InputLineIterator
    {
    public:
        static const bool debug = false;
        const size_t read_size = 2 * 1024 * 1024;

        virtual ~InputLineIterator() { }
    };

    //! InputLineIterator gives you access to lines of a file
    class InputLineIteratorUncompressed : public InputLineIterator
    {
    public:
        using Base = InputLineIterator;

        //! Creates an instance of iterator that reads file line based
        InputLineIteratorUncompressed(
            std::vector<FileSizePair> files,
            size_t my_id,
            size_t num_workers)
            : files_(files),
              my_id_(my_id),
              num_workers_(num_workers) {

            input_size_ = files[NumFiles()].second;

            // Go to start of 'local part'.
            auto my_start_and_end = common::CalculateLocalRange(input_size_, num_workers_, my_id_);

            size_t my_start = std::get<0>(my_start_and_end);
            my_end_ = std::get<1>(my_start_and_end);

            while (files_[current_file_ + 1].second <= my_start) {
                current_file_++;
            }
            if (my_start < my_end_) {
                LOG << "Opening file " << current_file_;
                c_file_ = OpenFile(files_[current_file_].first);
            }
            else {
                LOG << "my_start : " << my_start << " my_end_: " << my_end_;
                return;
            }

            // find offset in current file:
            // offset = start - sum of previous file sizes
            offset_ = lseek(c_file_, my_start - files_[current_file_].second, SEEK_CUR);
            buffer_.Reserve(Base::read_size);
            ssize_t buffer_size = read(c_file_, buffer_.data(), Base::read_size);
            buffer_.set_size(buffer_size);

            if (offset_ != 0) {
                bool found_n = false;

                // find next newline, discard all previous data as previous worker already covers it
                while (!found_n) {
                    for (auto it = buffer_.begin() + current_; it != buffer_.end(); it++) {
                        if (THRILL_UNLIKELY(*it == '\n')) {
                            current_ = it - buffer_.begin() + 1;
                            found_n = true;
                            break;
                        }
                    }
                    // no newline found: read new data into buffer_builder
                    if (!found_n) {
                        current_ = 0;
                        offset_ += buffer_.size();
                        buffer_size = read(c_file_, buffer_.data(), Base::read_size);
                        // EOF = newline per definition
                        if (!buffer_size) {
                            found_n = true;
                        }
                        buffer_.set_size(buffer_size);
                    }
                }
                assert(buffer_[current_ - 1] == '\n' || !buffer_size);
            }
        }

        //! returns the next element if one exists
        //!
        //! does no checks whether a next element exists!
        std::string Next() {
            std::string ret;
            while (true) {
                for (auto it = buffer_.begin() + current_; it != buffer_.end(); it++) {
                    if (THRILL_UNLIKELY(*it == '\n')) {
                        size_t strlen = it - buffer_.begin() - current_;
                        current_ = it - buffer_.begin() + 1;
                        LOG << "returning string";
                        return ret.append(buffer_.PartialToString(current_ - strlen - 1, strlen));
                    }
                }
                ret.append(buffer_.PartialToString(current_, buffer_.size() - current_));
                current_ = 0;
                ssize_t buffer_size = read(c_file_, buffer_.data(), Base::read_size);
                offset_ += buffer_.size();
                if (buffer_size) {
                    buffer_.set_size(buffer_size);
                }
                else {
                    close(c_file_);
                    current_file_++;
                    offset_ = 0;

                    if (current_file_ < NumFiles()) {
                        c_file_ = OpenFile(files_[current_file_].first);
                        ssize_t buffer_size = read(c_file_, buffer_.data(), Base::read_size);
                        buffer_.set_size(buffer_size);
                    }
                    else {
                        current_ = files_[current_file_].second - files_[current_file_ - 1].second;
                    }

                    if (ret.length()) {
                        LOG << "end - returning string of length" << ret.length();
                        return ret;
                    }
                }
            }
        }

        //! returns true, if an element is available in local part
        bool HasNext() {
            size_t global_index = offset_ + current_ + files_[current_file_].second;
            return global_index < my_end_ ||
                   (global_index == my_end_ && files_[current_file_ + 1].second - files_[current_file_].second > offset_ + current_);
        }

        size_t NumFiles() {
            return files_.size() - 1;
        }

        //! Open file and return file handle
        //! \param path Path to open
        int OpenFile(const std::string& path) {
            return open(path.c_str(), O_RDONLY);
        }

    private:
        //! Input files with size prefixsum.
        std::vector<FileSizePair> files_; // REVIEW(an): use a const & to the vector
        //! Index of current file in files_
        size_t current_file_ = 0;
        //! File handle to files_[current_file_]
        int c_file_;
        //! Offset of current block in c_file_.
        size_t offset_ = 0;
        //! Start of next element in current buffer.
        size_t current_ = 0;
        //! (exclusive) end of local block
        size_t my_end_;
        //! Byte buffer to create line-std::strings
        net::BufferBuilder buffer_;
        //! Worker ID
        size_t my_id_;
        //! total number of workers
        size_t num_workers_;
        //! Size of all files combined (in bytes)
        size_t input_size_;
    };

    //! InputLineIterator gives you access to lines of a file
    class InputLineIteratorCompressed : public InputLineIterator
    {
    public:
        using Base = InputLineIterator;

        //! Creates an instance of iterator that reads file line based
        InputLineIteratorCompressed(
            std::vector<FileSizePair> files,
            size_t my_id,
            size_t num_workers)
            : files_(files),
              my_id_(my_id),
              num_workers_(num_workers) {

            input_size_ = files[NumFiles()].second;

            // Go to start of 'local part'.
            size_t my_start;
            std::tie(my_start, my_end_) = common::CalculateLocalRange(input_size_, num_workers_, my_id_);

            while (files_[current_file_ + 1].second <= my_start) {
                current_file_++;
            }

            for (size_t file_nr = current_file_; file_nr < NumFiles(); file_nr++) {
                if (files[file_nr + 1].second == my_end_) {
                    break;
                }
                if (files[file_nr + 1].second > my_end_) {
                    my_end_ = files_[file_nr].second;
                    break;
                }
            }
			

            if (my_start < my_end_) {
                LOG << "Opening file " << current_file_;
                LOG << "my_start : " << my_start << " my_end_: " << my_end_;
                c_file_ = core::SysFile::OpenForRead(files_[current_file_].first);
            }
            else {
                // No local files, set buffer size to 2, so HasNext() does not try to read
                LOG << "my_start : " << my_start << " my_end_: " << my_end_;
                buffer_.Reserve(2);
                buffer_.set_size(2);
				current_ = buffer_.begin();
                return;
            }
            buffer_.Reserve(read_size);
            ssize_t buffer_size = c_file_.read(buffer_.data(), read_size);
            buffer_.set_size(buffer_size);
			current_ = buffer_.begin();
        }

        //! returns the next element if one exists
        //!
        //! does no checks whether a next element exists!
        std::string Next() {
            std::string ret;
            while (true) {
                for (auto it = current_; it != buffer_.end(); it++) {
                    if (THRILL_UNLIKELY(*it == '\n')) {
                        size_t strlen = it - current_;
                        current_ = it + 1;
						if (ret.size()) {
							return ret.append(it - strlen, it);
						} else {
							return std::string(it - strlen, it);
						}
                    }
                }
                ret.append(current_, buffer_.end());
                current_ = buffer_.begin();
                ssize_t buffer_size = c_file_.read(buffer_.data(), read_size);
                offset_ += buffer_.size();
                if (buffer_size) {
                    buffer_.set_size(buffer_size);
                }
                else {
                    LOG << "Opening new file!";
                    c_file_.close();
                    current_file_++;
                    offset_ = 0;

                    if (current_file_ < NumFiles()) {
                        c_file_ = core::SysFile::OpenForRead(files_[current_file_].first);
                        ssize_t buffer_size = c_file_.read(buffer_.data(), read_size);
                        buffer_.set_size(buffer_size);
                    }
                    else {
                        current_ = buffer_.begin();
                        // current_ = files_[current_file_].second - files_[current_file_ - 1].second;
                    }

                    if (ret.length()) {
                        LOG << "end - returning string of length" << ret.length();
                        return ret;
                    }
                }
            }
        }

        size_t NumFiles() {
            return files_.size() - 1;
        }

        //! returns true, if an element is available in local part
        bool HasNext() {
            // if block is fully read, read next block. needs to be done here
            // as HasNext() has to know if file is finished
            //         v-- no new line at end ||   v-- newline at end of file
            if (current_ >= buffer_.end() || (current_ + 1 >= buffer_.end() && *current_ == '\n')) {
                LOG << "New buffer in HasNext()";
                current_ = buffer_.begin();
                ssize_t buffer_size = c_file_.read(buffer_.data(), read_size);
                offset_ += buffer_.size();
                if (buffer_size > 1 || (buffer_size == 1 && buffer_[0] != '\n')) {
                    buffer_.set_size(buffer_size);
                    return true;
                }
                else {
                    LOG << "Opening new file in HasNext()";
                    // already at last file
                    if (current_file_ >= NumFiles() - 1) {
                        return false;
                    }
                    c_file_.close();
                    // if (this worker reads at least one more file)
                    if (my_end_ > files_[current_file_ + 1].second) {
                        current_file_++;
                        offset_ = 0;

                        c_file_ = core::SysFile::OpenForRead(files_[current_file_].first);
                        buffer_.set_size(c_file_.read(buffer_.data(), read_size));
                        return true;
                    }
                    else {
                        return false;
                    }
                }
            }
            else {
				return files_[current_file_].second < my_end_;
            }
        }

    private:
        //! Input files with size prefixsum.
        std::vector<FileSizePair> files_;
        //! Index of current file in files_
        size_t current_file_ = 0;
        //! File handle to files_[current_file_]
        core::SysFile c_file_;
        //! Offset of current block in c_file_.
        size_t offset_ = 0;
        //! Start of next element in current buffer.
		unsigned char* current_;
        //size_t current_ = 0;
        //! (exclusive) end of local block
        size_t my_end_;
        //! Byte buffer to create line-std::strings
        net::BufferBuilder buffer_;
        //! Worker ID
        size_t my_id_;
        //! total number of workers
        size_t num_workers_;
        //! Size of all files combined (in bytes)
        size_t input_size_;
    };
};

DIARef<std::string> ReadLines(Context& ctx, std::string filepath) {

    StatsNode* stats_node = ctx.stats_graph().AddNode(
        "ReadLines", DIANodeType::DOP);

    auto shared_node =
        std::make_shared<ReadLinesNode>(
            ctx, filepath, stats_node);

    auto read_stack = shared_node->ProduceStack();

    return DIARef<std::string, decltype(read_stack)>(
        shared_node, read_stack, { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_READ_LINES_HEADER

/******************************************************************************/
