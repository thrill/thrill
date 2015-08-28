/*******************************************************************************
 * thrill/api/read_binary.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
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
#include <thrill/data/block.hpp>
#include <thrill/data/block_reader.hpp>
#include <thrill/net/buffer_builder.hpp>

#include <algorithm>
#include <string>

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
        : Super(ctx, { }, "ReadBinary", stats_node),
          filepath_(filepath)
    {
        filelist_ = core::GlobFileSizePrefixSum(filepath_);

        size_t my_start, my_end;
        std::tie(my_start, my_end) =
            common::CalculateLocalRange(filelist_[filelist_.size() - 1].second,
                                        context_.num_workers(),
                                        context_.my_rank());
        size_t first_file = 0;
        size_t last_file = 0;

        while (filelist_[first_file + 1].second <= my_start) {
            first_file++;
            last_file++;
        }

        while (last_file < filelist_.size() - 1 && filelist_[last_file + 1].second <= my_end) {
            last_file++;
        }

        my_files_ = std::vector<FileSizePair>(
            filelist_.begin() + first_file,
            filelist_.begin() + last_file);

        LOG << my_files_.size() << " files from " << my_start << " to " << my_end;
    }

    virtual ~ReadBinaryNode() { }

    //! Executes the read operation. Reads a file line by line
    //! and emmits it after applyung the read function.
    void Execute() final { }

    void PushData() final {
        static const bool debug = false;
        LOG << "READING data " << result_file_.ToString();

        // Hook Read
        for (const FileSizePair& file : my_files_) {
            LOG << "OPENING FILE " << file.first;
            data::BlockReader<SysFileBlockSource> br(SysFileBlockSource(file.first, context_));
            while (br.HasNext()) {
                ValueType item = br.template NextNoSelfVerify<ValueType>();
                for (auto func : Super::callbacks_) {
                    func(item);
                }
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
        return "[ReadBinaryNode] Id: " + result_file_.ToString();
    }

private:
    //! Path of the input file.
    std::string filepath_;

    std::vector<FileSizePair> filelist_;
    std::vector<FileSizePair> my_files_;

    class SysFileBlockSource
    {
    public:
        const size_t read_size = 2 * 1024 * 1024;

        SysFileBlockSource(std::string path, Context& ctx) :
            context_(ctx), c_file_(core::SysFile::OpenForRead(path)) { }

        data::Block NextBlock() {
            if (done_) return data::Block();

            data::ByteBlockPtr bytes_ = data::ByteBlock::Allocate(read_size, context_.block_pool());

            ssize_t size = c_file_.read(bytes_->data(), read_size);
            if (size > 0) {
                return data::Block(bytes_, 0, size, 0, read_size);
            }
            else if (size < 0) {
                throw common::SystemException("File reading error", errno);
            }
            else {               //size == 0 -> read finished
                c_file_.close();
                done_ = true;
                return data::Block();
            }
        }

        bool closed() const {
            return done_;
        }

    protected:
        Context& context_;
        core::SysFile c_file_;
        bool done_ = false;
    };
};

/*!
 * ReadBinary is a DOp, which reads a file written by WriteBinary from the file
 * system and  creates an ordered DIA according to a given read function.
 *
 * \param ctx Reference to the context object
 * \param filepath Path of the file in the file system
 */
template <typename ValueType>
DIARef<ValueType> ReadBinary(Context& ctx, const std::string& filepath) {

    StatsNode* stats_node =
        ctx.stats_graph().AddNode("ReadBinary", NodeType::DOP);

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
