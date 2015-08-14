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

#include <fstream>
#include <string>
#include <glob.h>
#include <sys/stat.h>

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
		std::streampos directory_size = 0;

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
        static const bool debug = false;
        LOG << "READING data " << result_file_.ToString();

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

	std::vector<std::pair<std::string, std::streampos> > filesize_prefix;

    //! InputLineIterator gives you access to lines of a file
    class InputLineIterator
    {
    public:
        //! Creates an instance of iterator that reads file line based
        InputLineIterator(std::vector<std::pair<std::string, std::streampos>> files,
                          size_t my_id,
                          size_t num_workers)
            : files_(files),
              my_id_(my_id),
              num_workers_(num_workers) {
            // Find file size and save it
			
            file_size_ = files[files.size() - 1].second;

            // Go to start of 'local part'.
            std::streampos per_worker = file_size_ / num_workers_;
            std::streampos my_start = per_worker * my_id_;
            if (my_id_ == (num_workers - 1)) {
                my_end_ = file_size_ - 1;
            }
            else {
                my_end_ = per_worker * (my_id_ + 1) - 1;
            }

			while(files_[current_file_].second <= my_start) {
				current_file_++;
			}
			
			file_.open(files_[current_file_].first, std::ifstream::in);

			if (current_file_) {				
				file_.seekg(my_start - files_[current_file_ - 1].second, std::ios::beg);
				current_size_ = files_[current_file_].second - files_[current_file_ - 1].second;
			} else {				
				file_.seekg(my_start, std::ios::beg);
				current_size_ = files_[0].second;
			}

            // Go to next new line if the stream-pointer is not at the beginning
            // of a line
            if (file_.tellg() != 0) {
                std::streampos previous = file_.tellg() - (std::streampos) 1;
                file_.seekg(previous, std::ios::beg);
                if (file_.get() != '\n') {
                    std::string str;
                    std::getline(file_, str);
                }
				
            }
        }

        //! returns the next element if one exists
        //!
        //! does no checks whether a next element exists!
        std::string Next() {
			//go to next file
			if (file_.tellg() >= current_size_ - (std::streampos) 1) {
				file_.close();
				current_file_++;
				file_.open(files_[current_file_].first, std::ifstream::in);
				assert(file_.good());
				current_size_ = files_[current_file_].second - files_[current_file_ - 1].second;
			}
            std::string line;
            std::getline(file_, line);
            return line;
        }

        //! returns true, if an element is available in local part
        bool HasNext() {
            if (current_file_) {
				return (file_.tellg() + files_[current_file_ - 1].second <= my_end_);
			} else {
				return file_.tellg() <= my_end_;
			}
        }

    private:
        //! Input file stream
        std::vector<std::pair<std::string, std::streampos> > files_;
		//! Index of current file in files_
		size_t current_file_ = 0;
		//!Size of current file in bytes
		std::streampos current_size_;
		//! Current stream, from files_[current_file_]
		std::ifstream file_;
        //! File size in bytes
        size_t file_size_;
        //! Worker ID
        size_t my_id_;
        //! total number of workers
        size_t num_workers_;
        //! end of local block
        std::streampos my_end_;
    };

    //! Returns an InputLineIterator with a given input file stream.
    //!
    //! \param file Input file stream
    //! \param my_id Id of this worker
    //! \param num_work Number of workers
    //!
    //! \return An InputLineIterator for a given file stream
    InputLineIterator GetInputLineIterator(
        std::vector<std::pair<std::string, std::streampos> > files, size_t my_id, size_t num_work) {
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
