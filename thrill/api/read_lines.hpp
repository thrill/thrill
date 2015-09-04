/*******************************************************************************
 * thrill/api/read_lines.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_READ_LINES_HEADER
#define THRILL_API_READ_LINES_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/source_node.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <thrill/common/stat_logger.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>
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

    void PushData(bool /* consume */) final {
        if (contains_compressed_file_) {
            InputLineIteratorCompressed it = InputLineIteratorCompressed(
                filesize_prefix_, context_);

            // Hook Read
            while (it.HasNext()) {
                this->PushItem(it.Next());
            }
        }
        else {
            InputLineIteratorUncompressed it = InputLineIteratorUncompressed(
                filesize_prefix_, context_);

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

    class InputLineIterator
    {
    public:
        InputLineIterator(const std::vector<FileSizePair>& files, Context& ctx)
            : files_(files), context_(ctx) { }

        static const bool debug = false;

    protected:
        //! Block read size
        const size_t read_size = data::default_block_size;
        //! String, which Next() references to
        std::string data_;
        //! Input files with size prefixsum.
        const std::vector<FileSizePair>& files_;
        //! Index of current file in files_
        size_t current_file_ = 0;
        //! Byte buffer to create line-std::strings
        net::BufferBuilder buffer_;
        //! Start of next element in current buffer.
        unsigned char* current_;
        //! (exclusive) end of local block
        size_t my_end_;
		//! Reference to context
		Context& context_;

		size_t stats_total_bytes_ = 0;
		size_t stats_total_reads_ = 0;
		size_t stats_total_elements_ = 0;

		bool ReadBlock(core::SysFile& file, net::BufferBuilder& buffer) {
			ssize_t bytes = file.read(buffer.data(), read_size);
			if(bytes < 0) {
				throw common::SystemException("Read error", errno);
			}
			buffer.set_size(bytes);
			current_ = buffer.begin();
			stats_total_bytes_ += bytes;
			stats_total_reads_++;
			LOG << "Opening block with " << bytes << " bytes.";
			return bytes > 0;
		}

        ~InputLineIterator() {
            STAT(context_) << "NodeType" << "ReadLines"
                           << "TotalBytes" << stats_total_bytes_
                           << "TotalReads" << stats_total_reads_
                           << "TotalLines" << stats_total_elements_;
        }
    };

    //! InputLineIterator gives you access to lines of a file
    class InputLineIteratorUncompressed : public InputLineIterator
    {
    public:
        //! Creates an instance of iterator that reads file line based
        InputLineIteratorUncompressed(
            const std::vector<FileSizePair>& files,
            Context& ctx)
            : InputLineIterator(files, ctx) {

            // Go to start of 'local part'.
            size_t my_start;
            std::tie(my_start, my_end_) =
                common::CalculateLocalRange(files[NumFiles()].second, ctx.num_workers(), ctx.my_rank());

            while (files_[current_file_ + 1].second <= my_start) {
                current_file_++;
            }
            if (my_start < my_end_) {
                LOG << "Opening file " << current_file_;
                file_ = core::SysFile::OpenForRead(files_[current_file_].first);
            }
            else {
                LOG << "my_start : " << my_start << " my_end_: " << my_end_;
                return;
            }

            // find offset in current file:
            // offset = start - sum of previous file sizes
            offset_ = file_.lseek(my_start - files_[current_file_].second);
            buffer_.Reserve(read_size);
            ReadBlock(file_, buffer_);

            if (offset_ != 0) {
                bool found_n = false;

                // find next newline, discard all previous data as previous worker already covers it
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
                        if (!ReadBlock(file_, buffer_)) {
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
        const std::string & Next() {
            stats_total_elements_++;
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
                if (!ReadBlock(file_, buffer_)) {
					LOG << "opening next file";

                    file_.close();
                    current_file_++;
                    offset_ = 0;

                    if (current_file_ < NumFiles()) {
                        file_ = core::SysFile::OpenForRead(files_[current_file_].first);
						offset_ += buffer_.size();
                        ReadBlock(file_, buffer_);
                    }
                    else {
                        current_ = buffer_.begin() +
                                   files_[current_file_].second - files_[current_file_ - 1].second;
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
            size_t global_index = offset_ + position_in_buf + files_[current_file_].second;
            return global_index < my_end_ ||
                   (global_index == my_end_ &&
                    files_[current_file_ + 1].second - files_[current_file_].second >
                    offset_ + position_in_buf);
        }

        size_t NumFiles() {
            return files_.size() - 1;
        }

    private:
        //! Offset of current block in file_.
        size_t offset_ = 0;
        //! File handle to files_[current_file_]
        core::SysFile file_;
    };

    //! InputLineIterator gives you access to lines of a file
    class InputLineIteratorCompressed : public InputLineIterator
    {
    public:
        //! Creates an instance of iterator that reads file line based
        InputLineIteratorCompressed(
            const std::vector<FileSizePair>& files,
            Context& ctx)
            : InputLineIterator(files, ctx) {

            // Go to start of 'local part'.
            size_t my_start;
            std::tie(my_start, my_end_) =
                common::CalculateLocalRange(files[NumFiles()].second, ctx.num_workers(), ctx.my_rank());

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
                file_ = core::SysFile::OpenForRead(files_[current_file_].first);
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
            ReadBlock(file_, buffer_);
            data_.reserve(4 * 1024);
        }

        //! returns the next element if one exists
        //!
        //! does no checks whether a next element exists!
        const std::string & Next() {
            stats_total_elements_++;
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

                if (!ReadBlock(file_, buffer_)) {
                    LOG << "Opening new file!";
                    file_.close();
                    current_file_++;

                    if (current_file_ < NumFiles()) {
                        file_ = core::SysFile::OpenForRead(files_[current_file_].first);
                        ReadBlock(file_, buffer_);
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

        size_t NumFiles() {
            return files_.size() - 1;
        }

        //! returns true, if an element is available in local part
        bool HasNext() {
            if (files_[current_file_].second >= my_end_) {
                return false;
            }

            // if block is fully read, read next block. needs to be done here
            // as HasNext() has to know if file is finished
            //         v-- no new line at end ||   v-- newline at end of file
            if (current_ >= buffer_.end() || (current_ + 1 >= buffer_.end() && *current_ == '\n')) {
                LOG << "New buffer in HasNext()";
                ReadBlock(file_, buffer_);
                if (buffer_.size() > 1 || (buffer_.size() == 1 && buffer_[0] != '\n')) {
                    return true;
                }
                else {
                    LOG << "Opening new file in HasNext()";
                    // already at last file
                    if (current_file_ >= NumFiles() - 1) {
                        return false;
                    }
                    file_.close();
                    // if (this worker reads at least one more file)
                    if (my_end_ > files_[current_file_ + 1].second) {
                        current_file_++;
                        file_ = core::SysFile::OpenForRead(files_[current_file_].first);
                        ReadBlock(file_, buffer_);
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
        core::SysFile file_;
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
