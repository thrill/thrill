/*******************************************************************************
 * thrill/api/write_binary.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_WRITE_BINARY_HEADER
#define THRILL_API_WRITE_BINARY_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/core/stage_builder.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>

#include <fstream>
#include <iomanip>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIARef>
class WriteBinaryNode : public ActionNode
{
    static const bool debug = false;

public:
    using Super = ActionNode;
    using Super::context_;

    WriteBinaryNode(const ParentDIARef& parent,
                    const std::string& path_out,
                    size_t max_file_size,
                    StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() },
                     "WriteBinary", stats_node),
          out_pathbase_(path_out),
          max_file_size_(max_file_size)
    {
        sLOG << "Creating write node.";

        block_size_ = std::min(data::default_block_size,
                               common::RoundUpToPowerOfTwo(max_file_size));

        auto pre_op_fn = [=](const ValueType& input) {
                             return PreOp(input);
                         };
        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    //! Closes the output file
    void Execute() final {
        sLOG << "closing file" << out_pathbase_;
        writer_.reset();
        sink_.reset();
    }

    void Dispose() final { }

    std::string ToString() final {
        return "[WriteBinaryNode] Id:" + result_file_.ToString();
    }

protected:
    //! Implements BlockSink class writing to std::ofstream with size limit.
    class OStreamSink : public data::BoundedBlockSink
    {
    public:
        OStreamSink(data::BlockPool& block_pool,
                    const std::string& path, size_t max_file_size)
            : BlockSink(block_pool),
              BoundedBlockSink(block_pool, max_file_size),
              outstream_(path) { }

        void AppendBlock(const data::Block& b) final {
            outstream_.write(
                reinterpret_cast<const char*>(b.data_begin()), b.size());
        }

        void Close() final {
            outstream_.close();
        }

    protected:
        std::ofstream outstream_;
    };

    using Writer = data::BlockWriter<OStreamSink>;

    //! Base path of the output file.
    std::string out_pathbase_;

    //! File serial number for this worker
    size_t out_serial_ = 0;

    //! Maximum file size
    size_t max_file_size_;

    //! Block size used by BlockWriter
    size_t block_size_ = data::default_block_size;

    //! BlockSink which writes to an actual file
    std::unique_ptr<OStreamSink> sink_;

    //! BlockWriter to sink.
    std::unique_ptr<Writer> writer_;

    //! Function to create sink_ and writer_ for next file
    void OpenNextFile() {
        if (writer_) writer_.reset();

        std::ostringstream out_path;
        out_path << out_pathbase_
                 << '-'
                 << std::setw(5) << std::setfill('0') << context_.my_rank()
                 << '-'
                 << std::setw(10) << std::setfill('0') << out_serial_++;

        sink_ = std::make_unique<OStreamSink>(
            context_.block_pool(), out_path.str(), max_file_size_);

        writer_ = std::make_unique<Writer>(sink_.get(), block_size_);
    }

    //! writer preop: put item into file, create files as needed.
    void PreOp(const ValueType& input) {
        if (!sink_) OpenNextFile();

        try {
            writer_->PutItemNoSelfVerify(input);
        }
        catch (data::FullException& e) {
            // sink is full. flush it. and repeat, which opens new file.
            OpenNextFile();

            try {
                writer_->PutItemNoSelfVerify(input);
            }
            catch (data::FullException& e) {
                throw std::runtime_error(
                          "Error in WriteBinary: "
                          "an item is larger than the file size limit");
            }
        }
    }
};

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::WriteBinary(
    const std::string& filepath, size_t max_file_size) const {

    using WriteResultNode = WriteBinaryNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("WriteBinary", NodeType::ACTION);

    auto shared_node =
        std::make_shared<WriteResultNode>(
            *this, filepath, max_file_size, stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_WRITE_BINARY_HEADER

/******************************************************************************/
