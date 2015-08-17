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
#include <c7a/net/buffer_builder.hpp>
#include <c7a/common/logger.hpp>

#include <fstream>
#include <string>
#include <glob.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>


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

    /*!
     * Constructor for a ReadLinesNode. Sets the Context
     * and file path.
     *
     * \param ctx Reference to Context, which holds references to data and network.
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
		glob(path_.c_str(),GLOB_TILDE,NULL,&glob_result);
		size_t directory_size = 0;

		for(unsigned int i=0;i<glob_result.gl_pathc;++i){
			std::string filepath = std::string(glob_result.gl_pathv[i]);

			if (stat( filepath.c_str(), &filestat )) {				
				throw std::runtime_error("ERROR: Invalid file " + filepath);	
			}
			if (S_ISDIR( filestat.st_mode )) continue;

			directory_size += filestat.st_size;

			filesize_prefix.push_back(std::make_pair(filepath, directory_size));
		}
		globfree(&glob_result);	
	}

    virtual ~ReadLinesNode() { }

    //! Executes the read operation. Reads a file line by line
    //! and emmits it after applyung the read function.
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

    /*!
     * Returns "[ReadLinesNode]" as a string.
     * \return "[ReadLinesNode]"
     */
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
        InputLineIterator(std::vector<std::pair<std::string, size_t>> files,
                          size_t my_id,
                          size_t num_workers)
            : files_(files),
              my_id_(my_id),
              num_workers_(num_workers) {
	
            file_size_ = files[files.size() - 1].second;

            // Go to start of 'local part'.
            size_t per_worker = file_size_ / num_workers_;
            size_t my_start = per_worker * my_id_;
            if (my_id_ == (num_workers - 1)) {
                my_end_ = file_size_ - 1;
            }
            else {
                my_end_ = per_worker * (my_id_ + 1) - 1;
            }

			while(files_[current_file_].second <= my_start) {
				current_file_++;
			}

			c_file_ = open(files_[current_file_].first.c_str(), O_RDONLY);

			//find offset in current file:
			//offset = start - sum of previous file sizes 
			if (current_file_) {
				offset_ = lseek(c_file_, my_start - files_[current_file_ - 1].second, SEEK_CUR);
				current_size_ = files_[current_file_].second - files_[current_file_ - 1].second;
			} else {		
				offset_ = lseek(c_file_, my_start, SEEK_CUR);
				current_size_ = files_[0].second;
			}


			if (offset_ != 0) {
				offset_ = lseek(c_file_, -1, SEEK_CUR);	
				bb_.Reserve(read_size);
				ssize_t buffer_size = read(c_file_, bb_.data(), read_size);
				bb_.set_size(buffer_size);
				buffer_ = bb_.ToBuffer();
				current_ = 1;
				if (buffer_[0] != '\n') {
					bool found_n = false;
					//find next newline, discard all previous data as previous worker already covers it
					while (!found_n) {
						for (auto it = buffer_.begin() + current_; it != buffer_.end(); it++) { 
							if (*it == '\n') {
								current_ = it - buffer_.begin() + 1;
								found_n = true;
								break;
							}
						}
						//no newline found: read new data into buffer_builder
						if (!found_n) {
							current_ = 0;							
							offset_ += buffer_.size();
							bb_.Reserve(read_size);
							buffer_size = read(c_file_, bb_.data(), read_size);
							bb_.set_size(buffer_size);
							buffer_ = bb_.ToBuffer();
						}
					}					
					assert(buffer_[current_ - 1] == '\n');
				}
            } else {
				bb_.Reserve(read_size);
				ssize_t buffer_size = read(c_file_, bb_.data(), read_size);
				bb_.set_size(buffer_size);
				buffer_ = bb_.ToBuffer();
			}
        }

        //! returns the next element if one exists
        //!
        //! does no checks whether a next element exists!
        std::string Next() {
			while (true) {
				std::string ret;
				for (auto it = buffer_.begin() + current_; it != buffer_.end(); it++) { 
					if (*it == '\n') {
						size_t strlen = it - buffer_.begin() - current_;
						current_ = it - buffer_.begin() + 1;
						return ret.append(buffer_.PartialToString(current_ - strlen - 1, strlen));
					}
				}
				ret.append(buffer_.PartialToString(current_, buffer_.size() - current_));
				current_ = 0;
				bb_.Reserve(read_size);
				ssize_t buffer_size = read(c_file_, bb_.data(), read_size);
				offset_ += buffer_.size();
				if (buffer_size) {
					bb_.set_size(buffer_size);
					buffer_ = bb_.ToBuffer();
				} else {
					close(c_file_);
					current_file_++;
					offset_ = 0;
					c_file_ = open(files_[current_file_].first.c_str(), O_RDONLY);					
					bb_.Reserve(read_size);
					ssize_t buffer_size = read(c_file_, bb_.data(), read_size);
					bb_.set_size(buffer_size);
					buffer_ = bb_.ToBuffer();
					
					if (ret.length()) {
						return ret;
					}
				}
			}
        }

        //! returns true, if an element is available in local part
        bool HasNext() {
            if (current_file_) {
				return (offset_ + current_ + files_[current_file_ - 1].second <= my_end_);
			} else {
				return offset_ + current_ <= my_end_;
			}
        }

    private:
        //! Input file stream
        std::vector<std::pair<std::string, size_t> > files_;
		//! Index of current file in files_
		size_t current_file_ = 0;
		//!Size of current file in bytes
		size_t current_size_;
		//! Current stream, from files_[current_file_]
		std::ifstream file_;

		int c_file_;
		size_t offset_;
        //! File size in bytes
        size_t file_size_;
        //! Worker ID
        size_t my_id_;
        //! total number of workers
        size_t num_workers_;
        //! end of local block
        size_t my_end_;

		net::BufferBuilder bb_;
		net::Buffer buffer_;
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
        std::vector<std::pair<std::string, size_t> > files, size_t my_id, size_t num_work) {
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
