/*******************************************************************************
 * thrill/api/write_lines_many.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_WRITE_LINES_MANY_HEADER
#define THRILL_API_WRITE_LINES_MANY_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/core/file_io.hpp>
#include <thrill/core/stage_builder.hpp>
#include <thrill/net/buffer_builder.hpp>

#include <fstream>
#include <string>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIARef>
class WriteLinesManyNode : public ActionNode
{
    static const bool debug = false;

public:
    using Super = ActionNode;
    using Super::context_;

    WriteLinesManyNode(const ParentDIARef& parent,
                       const std::string& path_out,
                       size_t target_file_size,
                       StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, stats_node),
          out_pathbase_(path_out),
          file_(core::SysFile::OpenForWrite(core::make_path(
                                                out_pathbase_,
                                                context_.my_rank(),
                                                0))),
          target_file_size_(target_file_size)
    {
        sLOG << "Creating write node.";

        auto pre_op_fn = [=](std::string input) {
                             PreOp(input);
                         };

        max_buffer_size_ = std::min(data::default_block_size,
                                    common::RoundUpToPowerOfTwo(target_file_size_));

        write_buffer_.Reserve(max_buffer_size_);

        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    void PreOp(std::string input) {
        stats_total_elements_++;

        if (THRILL_UNLIKELY(current_buffer_size_ + input.size() + 1
                            >= max_buffer_size_)) {
            stats_total_writes_++;
            stats_total_bytes_ += current_buffer_size_;
            file_.write(write_buffer_.data(), current_buffer_size_);
            write_buffer_.set_size(0);
            current_file_size_ += current_buffer_size_;
            current_buffer_size_ = 0;
            if (THRILL_UNLIKELY(current_file_size_ >= target_file_size_)) {
                LOG << "Closing file" << out_serial_;
                file_.close();
                std::string new_path = core::make_path(
                    out_pathbase_, context_.my_rank(), out_serial_++);
                file_ = core::SysFile::OpenForWrite(new_path);
                LOG << "Opening file: " << new_path;
                current_file_size_ = 0;
            }
            // String is too long to fit into buffer, write directly, add '\n' to
            // start of next buffer.
            if (THRILL_UNLIKELY(input.size() >= max_buffer_size_)) {
                stats_total_writes_++;
                stats_total_bytes_ += input.size();
                current_file_size_ += input.size() + 1;
                file_.write(input.data(), input.size());
                current_buffer_size_ = 1;
                write_buffer_.PutByte('\n');
                return;
            }
        }
        current_buffer_size_ += input.size() + 1;
        write_buffer_.AppendString(input);
        write_buffer_.PutByte('\n');
        assert(current_buffer_size_ == write_buffer_.size());
    }

    //! Closes the output file, write last buffer
    void Execute() final {
        sLOG << "closing file";
        stats_total_writes_++;
        stats_total_bytes_ += current_buffer_size_;
        file_.write(write_buffer_.data(), current_buffer_size_);
        file_.close();

        STAT(context_) << "NodeType" << "WriteLinesMany"
             << "TotalBytes" << stats_total_bytes_
             << "TotalLines" << stats_total_elements_
             << "TotalWrites" << stats_total_writes_
             << "TotalFiles" << out_serial_;
    }

    void Dispose() final { }

private:
    //! Base path of the output file.
    std::string out_pathbase_;

    //! Current file size in bytes
    size_t current_file_size_ = 0;

    //! File serial number for this worker
    size_t out_serial_ = 1;

    //! File to wrtie to
    core::SysFile file_;

    //! Write buffer
    net::BufferBuilder write_buffer_;

    //! Maximum buffer size
    size_t max_buffer_size_;

    //! Current buffer size
    size_t current_buffer_size_ = 0;

    //! Targetl file size in bytes
    size_t target_file_size_;

    size_t stats_total_bytes_ = 0;
    size_t stats_total_elements_ = 0;
    size_t stats_total_writes_ = 0;
};

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::WriteLinesMany(
    const std::string& filepath, size_t target_file_size) const {
    assert(IsValid());

    static_assert(std::is_same<ValueType, std::string>::value,
                  "WriteLinesMany needs an std::string as input parameter");

    using WriteResultNode = WriteLinesManyNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("WriteLinesMany", DIANodeType::ACTION);

    auto shared_node =
        std::make_shared<WriteResultNode>(*this,
                                          filepath,
                                          target_file_size,
                                          stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_WRITE_LINES_MANY_HEADER

/******************************************************************************/
