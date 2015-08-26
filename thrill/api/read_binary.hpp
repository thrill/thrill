/*******************************************************************************
 * thrill/api/read_binary.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_READ_BINARY_HEADER
#define THRILL_API_READ_BINARY_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/item_serialization_tools.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <thrill/core/file_io.hpp>
#include <thrill/net/buffer_builder.hpp>

#include <algorithm>
#include <fcntl.h>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a line-based Read operation. Read reads a file from
 * the file system and emits it as a DIA.
 */
template <typename ValueType>
class ReadBinaryNode : public DOpNode<ValueType>
{
public:
    using Super = DOpNode<ValueType>;
    using Super::context_;
    using Super::result_file_;

    using FileSizePair = std::pair<std::string, size_t>;

    static const bool debug = false;

    /*!
     * Constructor for a ReadLinesNode. Sets the Context
     * and file path.
     *
     * \param ctx Reference to Context, which holds references to data and network.
     * \param filepath Path of the input file
     */
    ReadBinaryNode(Context& ctx,
                   const std::string& filepath,
                   StatsNode* stats_node)
        : Super(ctx, { }, "Read", stats_node),
          filepath_(filepath)
    {
		core::FileIO fio;
        filelist_ = fio.ReadFileList(filepath_).first;
        filesize_ = filelist_[context_.my_rank() + 1].second -
                    filelist_[context_.my_rank()].second;

        auto my_start_and_end =
            common::CalculateLocalRange(filelist_[filelist_.size() - 1].second,
                                        context_.num_workers(),
                                        context_.my_rank());

        size_t my_start = std::get<0>(my_start_and_end);
        size_t my_end = std::get<1>(my_start_and_end);
        size_t first_file = 0;
        size_t last_file = 0;

        while (filelist_[first_file + 1].second <= my_start) {
            first_file++;
            last_file++;
        }

        while (filelist_[last_file + 1].second <= my_end && last_file < filelist_.size() - 1) {
            last_file++;
        }

        auto start_iter = filelist_.begin() + first_file;
        auto end_iter = filelist_.begin() + last_file + 1;

        my_files_ = std::vector<FileSizePair>(start_iter, end_iter);
    }

    virtual ~ReadBinaryNode() { }

    //! Executes the read operation. Reads a file line by line
    //! and emmits it after applyung the read function.
    void Execute() final { }

    void PushData() final {
        static const bool debug = false;
        LOG << "READING data " << result_file_.ToString();

        bfr_.SetFileList(my_files_);

        // Hook Read
        while (bfr_.HasNext()) {
            auto item = data::Serialization<BinaryFileReader, ValueType>
                        ::Deserialize(bfr_);
			LOG << item;
            for (auto func : Super::callbacks_) {
                func(item);
            }
        }
		LOG << "DONE!";
    }

    void Dispose() final { }

    /*!
     * Produces an 'empty' function stack, which only contains the identity
     * emitter function.
     *
     * \return Empty function stack
     */
    auto ProduceStack() {
        return FunctionStack<ValueType>();
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
    std::string filepath_;

    std::streampos filesize_;

    std::vector<FileSizePair> filelist_;
    std::vector<FileSizePair> my_files_;

    class BinaryFileReader
        : public common::ItemReaderToolsBase<BinaryFileReader>
    {
    public:

        const size_t read_size = 2 * 1024;

        BinaryFileReader() { }

        virtual ~BinaryFileReader() { }

        void CloseStream() {
			close(c_file_);
        }

        void SetFileList(std::vector<FileSizePair> path) {
            filelist_ = path;
			if (filelist_.size() > 1) {
				c_file_ = fio.OpenFile(filelist_[0].first);
				buffer_.Reserve(read_size);
				current_size_ = filelist_[1].second - filelist_[0].second;
			}
        }

        bool HasNext() {
			//no input files for this worker
			if (filelist_.size() <= 1) {
				return false;
			}
			
			if (buffer_.size() == current_) {
				buffer_.set_size(read(c_file_, buffer_.data(), read_size));
				current_ = 0;
				//buffer is empty when file is already finished
				//->go to next file if there is another one in filelist_
				if (buffer_.size()) {
					return true;
				} else if (current_file_ < filelist_.size() - 2) {
					close(c_file_);
					current_file_++;
					current_size_ = filelist_[current_file_ + 1].second - filelist_[current_file_].second;
					c_file_ = fio.OpenFile(filelist_[current_file_].first);
					return true;
				} else {
					return false;
				}
			} else {
				return true;
			}
        }

        char GetByte() {
			if (current_ == buffer_.size()) {
				buffer_.set_size(read(c_file_, buffer_.data(), read_size));
				current_ = 0;				
			}
			current_++;
            return buffer_[current_ - 1];
        }

        template <typename Type>
        Type Get() {
			if (buffer_.size() < current_ + sizeof(Type)) {
				//copy rest of buffer into start of next buffer
				std::copy(buffer_.begin() + current_,
						  buffer_.end(),
						  buffer_.data());
				size_t copied_bytes = buffer_.end() - buffer_.begin() - current_; 

				buffer_.set_size(
					read(c_file_,
						 buffer_.data() + copied_bytes,
						 read_size - copied_bytes) + copied_bytes);
				current_ = 0;
			} 

			Type elemc;
			std::copy(buffer_.begin() + current_,
					  buffer_.begin() + current_ + sizeof(Type),
					  reinterpret_cast<char*>(&elemc));
			current_ += sizeof(Type);
			return elemc;
        }

    private:
		int c_file_;
		size_t current_ = 0;
		net::BufferBuilder buffer_;
        std::vector<FileSizePair> filelist_;
		std::streampos current_size_;
		size_t current_file_ = 0;
		core::FileIO fio;
    };

    BinaryFileReader bfr_;
};

template <typename ValueType>
DIARef<ValueType> ReadBinary(Context& ctx, std::string filepath, ValueType) {

    StatsNode* stats_node = ctx.stats_graph().AddNode("ReadBinary", NodeType::DOP);

    auto shared_node =
        std::make_shared<ReadBinaryNode<ValueType> >(
            ctx, filepath, stats_node);

    auto read_stack = shared_node->ProduceStack();

    return DIARef<ValueType, decltype(read_stack)>(
        shared_node, read_stack, { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_READ_BINARY_HEADER

/******************************************************************************/
