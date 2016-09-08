/*******************************************************************************
 * thrill/api/write_lines.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_WRITE_LINES_HEADER
#define THRILL_API_WRITE_LINES_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/common/math.hpp>
#include <thrill/core/file_io.hpp>
#include <thrill/net/buffer_builder.hpp>

#include <algorithm>
#include <string>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType>
class WriteLinesNode final : public ActionNode
{
    static constexpr bool debug = false;

public:
    using Super = ActionNode;
    using Super::context_;

    //! input type is the parent's output value type.
    using ValueType_ = ValueType;

    template <typename ParentDIA>
    WriteLinesNode(const ParentDIA& parent,
                   const std::string& path_out,
                   size_t target_file_size)
        : Super(parent.ctx(), "WriteLines",
                { parent.id() }, { parent.node() }),
          out_pathbase_(path_out),
          file_(core::SysFile::OpenForWrite(
                    core::FillFilePattern(
                        out_pathbase_, context_.my_rank(), 0))),
          target_file_size_(target_file_size)
    {
        sLOG << "Creating write node.";

        auto pre_op_fn = [this](const std::string& input) {
                             PreOp(input);
                         };

        max_buffer_size_ =
            std::min(data::default_block_size,
                     common::RoundUpToPowerOfTwo(target_file_size_));

        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    DIAMemUse PreOpMemUse() final {
        return max_buffer_size_;
    }

    void StartPreOp(size_t /* id */) final {
        write_buffer_.Reserve(max_buffer_size_);
    }

    void PreOp(const std::string& input) {
        stats_total_elements_++;

        if (THRILL_UNLIKELY(current_buffer_size_ + input.size() + 1
                            >= max_buffer_size_)) {
            stats_total_writes_++;
            stats_total_bytes_ += current_buffer_size_;
            timer.Start();
            file_->write(write_buffer_.data(), current_buffer_size_);
            timer.Stop();
            write_buffer_.set_size(0);
            current_file_size_ += current_buffer_size_;
            current_buffer_size_ = 0;
            if (THRILL_UNLIKELY(current_file_size_ >= target_file_size_)) {
                LOG << "Closing file" << out_serial_;
                file_->close();
                std::string new_path = core::FillFilePattern(
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
                timer.Start();
                file_->write(input.data(), input.size());
                timer.Stop();
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
    void StopPreOp(size_t /* id */) final {
        sLOG << "closing file";
        stats_total_writes_++;
        stats_total_bytes_ += current_buffer_size_;
        timer.Start();
        file_->write(write_buffer_.data(), current_buffer_size_);
        timer.Stop();
        file_->close();

        Super::logger_
            << "class" << "WriteLinesNode"
            << "event" << "done"
            << "total_bytes" << stats_total_bytes_
            << "total_lines" << stats_total_elements_
            << "total_writes" << stats_total_writes_
            << "total_files" << out_serial_
            << "write_time" << timer.Milliseconds();
    }

    void Execute() final { }

private:
    //! Base path of the output file.
    std::string out_pathbase_;

    //! Current file size in bytes
    size_t current_file_size_ = 0;

    //! File serial number for this worker
    size_t out_serial_ = 1;

    //! File to wrtie to
    std::shared_ptr<core::SysFile> file_;

    //! Write buffer
    net::BufferBuilder write_buffer_;

    //! Maximum buffer size
    size_t max_buffer_size_;

    //! Current buffer size
    size_t current_buffer_size_ = 0;

    //! Targetl file size in bytes
    size_t target_file_size_;

    common::StatsTimerStopped timer;

    size_t stats_total_bytes_ = 0;
    size_t stats_total_elements_ = 0;
    size_t stats_total_writes_ = 0;
};

template <typename ValueType, typename Stack>
void DIA<ValueType, Stack>::WriteLines(
    const std::string& filepath, size_t target_file_size) const {
    assert(IsValid());

    static_assert(std::is_same<ValueType, std::string>::value,
                  "WriteLines needs an std::string as input parameter");

    using WriteLinesNode = api::WriteLinesNode<ValueType>;

    auto node = common::MakeCounting<WriteLinesNode>(
        *this, filepath, target_file_size);

    node->RunScope();
}

template <typename ValueType, typename Stack>
Future<void> DIA<ValueType, Stack>::WriteLines(
    const struct FutureTag&,
    const std::string& filepath, size_t target_file_size) const {
    assert(IsValid());

    static_assert(std::is_same<ValueType, std::string>::value,
                  "WriteLines needs an std::string as input parameter");

    using WriteLinesNode = api::WriteLinesNode<ValueType>;

    auto node = common::MakeCounting<WriteLinesNode>(
        *this, filepath, target_file_size);

    return Future<void>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_WRITE_LINES_HEADER

/******************************************************************************/
