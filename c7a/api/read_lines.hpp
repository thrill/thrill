/*******************************************************************************
 * c7a/api/read_lines.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_READ_LINES_HEADER
#define C7A_API_READ_LINES_HEADER

#include <c7a/api/dia.hpp>
#include <c7a/api/dop_node.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/net/buffer_builder.hpp>
// C7A_{/UN}LIKELY
#include <c7a/common/math.hpp>
#include <c7a/common/item_serialization_tools.hpp>

#include <fcntl.h>
#include <fstream>
#include <glob.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a line-based Read operation. Read reads a file from
 * the file system and emits it as a DIA.
 */
class ReadLinesNode : public DOpNode<std::string>
{
public:
    using Super = DOpNode<std::string>;
    using Super::context_;
    using Super::result_file_;
	using FileSizePair = std::pair<std::string, size_t>;

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
        : Super(ctx, { }, "Read", stats_node),
          path_(path)
    {
        glob_t glob_result;
        struct stat filestat;
        glob(path_.c_str(), GLOB_TILDE, nullptr, &glob_result);
        size_t directory_size = 0;

        for (unsigned int i = 0; i < glob_result.gl_pathc; ++i) {
            const char* filepath = glob_result.gl_pathv[i];

            if (stat(filepath, &filestat)) {
                throw std::runtime_error(
                    "ERROR: Invalid file " + std::string(filepath));
            }
            if (!S_ISREG(filestat.st_mode)) continue;

            directory_size += filestat.st_size;

            filesize_prefix.emplace_back(std::move(filepath), directory_size);
        }
        globfree(&glob_result);
    }

    void Execute() final { }

    void PushData() final {
        InputLineIterator it = GetInputLineIterator(
            filesize_prefix, context_.my_rank(), context_.num_workers());

        // Hook Read
        while (it.HasNext()) {
            auto item = it.Next();
            for (auto func : Super::callbacks_) {
                func(item);
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

    std::string ToString() final {
        return "[ReadLinesNode] Id: " + result_file_.ToString();
    }

private:
    //! Path of the input file.
    std::string path_;

    std::vector<std::pair<std::string, size_t> > filesize_prefix;

    //! InputLineIterator gives you access to lines of a file
    class InputLineIterator
    {
    public:
        const size_t read_size = 2 * 1024 * 1024;

        //! Creates an instance of iterator that reads file line based
        InputLineIterator(
            const std::vector<FileSizePair >& files,
            size_t my_id,
            size_t num_workers)
            : files_(files),
              my_id_(my_id),
              num_workers_(num_workers) {

            input_size_ = files[files.size() - 1].second;

            // Go to start of 'local part'.
			auto my_start_and_end = common::CalculateLocalRange(input_size_, num_workers_, my_id_);
			
			size_t my_start = std::get<0>(my_start_and_end);
			my_end_ = std::get<1>(my_start_and_end);

            while (files_[current_file_].second <= my_start) {
                current_file_++;
            }

            c_file_ = OpenFile(files_[current_file_].first);

            // find offset in current file:
            // offset = start - sum of previous file sizes
            if (current_file_) {
                offset_ = lseek(c_file_, my_start - files_[current_file_ - 1].second, SEEK_CUR);
                current_size_ = files_[current_file_].second - files_[current_file_ - 1].second;
            }
            else {
                offset_ = lseek(c_file_, my_start, SEEK_CUR);
                current_size_ = files_[0].second;
            }

            if (offset_ != 0) {
                offset_ = lseek(c_file_, -1, SEEK_CUR);
                bb_.Reserve(read_size);
                ssize_t buffer_size = read(c_file_, bb_.data(), read_size);
                bb_.set_size(buffer_size);
                current_ = 1;

				//Move to next newline, if local part does not start at the beginning of a line.
                if (bb_[0] != '\n') {
                    bool found_n = false;

                    // find next newline, discard all previous data as previous worker already covers it
                    while (!found_n) {
                        for (auto it = bb_.begin() + current_; it != bb_.end(); it++) {
                            if (C7A_UNLIKELY(*it == '\n')) {
                                current_ = it - bb_.begin() + 1;
                                found_n = true;
                                break;
                            }
                        }
                        // no newline found: read new data into buffer_builder
                        if (!found_n) {
                            current_ = 0;
                            offset_ += bb_.size();
                            buffer_size = read(c_file_, bb_.data(), read_size);
							//EOF = newline per definition
							if (!buffer_size) {
								found_n = true;
							}
                            bb_.set_size(buffer_size);
                        }
                    }
                    assert(bb_[current_ - 1] == '\n' || !buffer_size);
                }
            }
            else {				
                bb_.Reserve(read_size);
                ssize_t buffer_size = read(c_file_, bb_.data(), read_size);
                bb_.set_size(buffer_size);
            }
        }

        //! returns the next element if one exists
        //!
        //! does no checks whether a next element exists!
        std::string Next() {
            while (true) {
                std::string ret;
                for (auto it = bb_.begin() + current_; it != bb_.end(); it++) {
                    if (C7A_UNLIKELY(*it == '\n')) {
                        size_t strlen = it - bb_.begin() - current_;
                        current_ = it - bb_.begin() + 1;
                        return ret.append(bb_.PartialToString(current_ - strlen - 1, strlen));
                    }
                }
                ret.append(bb_.PartialToString(current_, bb_.size() - current_));
                current_ = 0;
                ssize_t buffer_size = read(c_file_, bb_.data(), read_size);
                offset_ += bb_.size();
                if (buffer_size) {
                    bb_.set_size(buffer_size);
                }
                else {
                    close(c_file_);
                    current_file_++;
                    offset_ = 0;

                    // REVIEW(an): you must extract all the open() commands
                    // (this and first) into a function OpenFile() if we are to
                    // add decompressors.

                    c_file_ = OpenFile(files_[current_file_].first);
                    ssize_t buffer_size = read(c_file_, bb_.data(), read_size);
                    bb_.set_size(buffer_size);

                    if (ret.length()) {
                        return ret;
                    }
                }
            }
        }

        //! returns true, if an element is available in local part
        bool HasNext() {
            if (current_file_) {
                return (offset_ + current_ + files_[current_file_ - 1].second < my_end_);
            }
            else {
                return offset_ + current_ < my_end_;
            }
        }

		//! Open file and return file handle
		//! \param path Path to open
		int OpenFile(const std::string& path) {
			return open(path.c_str(), O_RDONLY);
		}

    private:
        //! Input files with inclusive size prefixsum.
        std::vector<FileSizePair> files_;
        //! Index of current file in files_
        size_t current_file_ = 0;
        //! Size of current file in bytes
        size_t current_size_;
		//! File handle to files_[current_file_]
        int c_file_;
		//! Offset of current block in c_file_.
        size_t offset_;
        //! Size of all files combined (in bytes)
        size_t input_size_;
        //! Worker ID
        size_t my_id_;
        //! total number of workers
        size_t num_workers_;
        //! (exclusive) end of local block
        size_t my_end_;
		//! Byte buffer to create line-std::strings
        net::BufferBuilder bb_;
		//! Start of next element in current buffer.
        size_t current_ = 0;
    };

    //! Returns an InputLineIterator with a given input file stream.
    //!
    //! \param file Input file stream
    //! \param my_id Id of this worker
    //! \param num_work Number of workers
    //!
    //! \return An InputLineIterator for a given file stream
    InputLineIterator GetInputLineIterator(
        // REVIEW(an): please make some using typedefs!
        std::vector<FileSizePair> files, size_t my_id, size_t num_work) {
        return InputLineIterator(files, my_id, num_work);
    }
};

DIARef<std::string> ReadLines(Context& ctx, std::string filepath) {

    StatsNode* stats_node = ctx.stats_graph().AddNode("ReadLines", NodeType::DOP);

    auto shared_node =
        std::make_shared<ReadLinesNode>(
            ctx, filepath, stats_node);

    auto read_stack = shared_node->ProduceStack();

    return DIARef<std::string, decltype(read_stack)>(
        shared_node, read_stack, { stats_node });
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_READ_LINES_HEADER

/******************************************************************************/
