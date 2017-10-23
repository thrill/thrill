/*******************************************************************************
 * thrill/api/write_binary.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_WRITE_BINARY_HEADER
#define THRILL_API_WRITE_BINARY_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/context.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/common/string.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/vfs/file_io.hpp>
#include <tlx/math/round_to_power_of_two.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType>
class WriteBinaryNode final : public ActionNode
{
    static constexpr bool debug = false;

public:
    using Super = ActionNode;
    using Super::context_;

    template <typename ParentDIA>
    WriteBinaryNode(const ParentDIA& parent,
                    const std::string& path_out,
                    size_t max_file_size)
        : ActionNode(parent.ctx(), "WriteBinary",
                     { parent.id() }, { parent.node() }),
          out_pathbase_(path_out),
          max_file_size_(max_file_size)
    {
        sLOG << "Creating write node.";

        block_size_ = std::min(data::default_block_size,
                               tlx::round_up_to_power_of_two(max_file_size));
        sLOG << "block_size_" << block_size_;

        auto pre_op_fn = [=](const ValueType& input) {
                             return PreOp(input);
                         };
        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    DIAMemUse PreOpMemUse() final {
        return data::default_block_size;
    }

    //! writer preop: put item into file, create files as needed.
    void PreOp(const ValueType& input) {
        stats_total_elements_++;

        if (!writer_) OpenNextFile();

        try {
            writer_->PutNoSelfVerify(input);
        }
        catch (data::FullException&) {
            // sink is full. flush it. and repeat, which opens new file.
            OpenNextFile();

            try {
                writer_->PutNoSelfVerify(input);
            }
            catch (data::FullException&) {
                throw std::runtime_error(
                          "Error in WriteBinary: "
                          "an item is larger than the file size limit");
            }
        }
    }

    //! Closes the output file
    void StopPreOp(size_t /* id */) final {
        sLOG << "closing file" << out_pathbase_;
        writer_.reset();

        Super::logger_
            << "class" << "WriteBinaryNode"
            << "total_elements" << stats_total_elements_
            << "total_writes" << stats_total_writes_;
    }

    void Execute() final { }

private:
    //! Implements BlockSink class writing to files with size limit.
    class SysFileSink final : public data::BoundedBlockSink
    {
    public:
        SysFileSink(api::Context& context,
                    size_t local_worker_id,
                    const std::string& path, size_t max_file_size,
                    size_t& stats_total_elements,
                    size_t& stats_total_writes)
            : BlockSink(context.block_pool(), local_worker_id),
              BoundedBlockSink(context.block_pool(), local_worker_id, max_file_size),
              stream_(vfs::OpenWriteStream(path)),
              stats_total_elements_(stats_total_elements),
              stats_total_writes_(stats_total_writes) { }

        void AppendPinnedBlock(
            const data::PinnedBlock& b, bool /* is_last_block */) final {
            sLOG << "SysFileSink::AppendBlock()" << b;
            stats_total_writes_++;
            stream_->write(b.data_begin(), b.size());
        }

        void AppendPinnedBlock(data::PinnedBlock&& b, bool is_last_block) final {
            return AppendPinnedBlock(b, is_last_block);
        }

        void AppendBlock(const data::Block& block, bool is_last_block) {
            return AppendPinnedBlock(
                block.PinWait(local_worker_id()), is_last_block);
        }

        void AppendBlock(data::Block&& block, bool is_last_block) {
            return AppendPinnedBlock(
                block.PinWait(local_worker_id()), is_last_block);
        }

        void Close() final {
            stream_->close();
        }

    private:
        vfs::WriteStreamPtr stream_;
        size_t& stats_total_elements_;
        size_t& stats_total_writes_;
    };

    using Writer = data::BlockWriter<SysFileSink>;

    //! Base path of the output file.
    std::string out_pathbase_;

    //! File serial number for this worker
    size_t out_serial_ = 0;

    //! Maximum file size
    size_t max_file_size_;

    //! Block size used by BlockWriter
    size_t block_size_ = data::default_block_size;

    //! BlockWriter to sink.
    std::unique_ptr<Writer> writer_;

    size_t stats_total_elements_ = 0;
    size_t stats_total_writes_ = 0;

    //! Function to create sink_ and writer_ for next file
    void OpenNextFile() {
        writer_.reset();

        // construct path from pattern containing ### and $$$
        std::string out_path = vfs::FillFilePattern(
            out_pathbase_, context_.my_rank(), out_serial_++);

        sLOG << "OpenNextFile() out_path" << out_path;

        writer_ = std::make_unique<Writer>(
            SysFileSink(
                context_, context_.local_worker_id(),
                out_path, max_file_size_,
                stats_total_elements_, stats_total_writes_),
            block_size_);
    }
};

template <typename ValueType, typename Stack>
void DIA<ValueType, Stack>::WriteBinary(
    const std::string& filepath, size_t max_file_size) const {

    using WriteBinaryNode = api::WriteBinaryNode<ValueType>;

    auto node = tlx::make_counting<WriteBinaryNode>(
        *this, filepath, max_file_size);

    node->RunScope();
}

template <typename ValueType, typename Stack>
Future<void> DIA<ValueType, Stack>::WriteBinaryFuture(
    const std::string& filepath, size_t max_file_size) const {

    using WriteBinaryNode = api::WriteBinaryNode<ValueType>;

    auto node = tlx::make_counting<WriteBinaryNode>(
        *this, filepath, max_file_size);

    return Future<void>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_WRITE_BINARY_HEADER

/******************************************************************************/
