/*******************************************************************************
 * thrill/api/write_binary.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
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
                    StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() },
                     "WriteBinary", stats_node),
          path_out_(path_out + std::to_string(context_.my_rank()))
    {
        sLOG << "Creating write node.";

        auto pre_op_fn = [=](const ValueType& input) {
                             writer_(input);
                         };
        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    //! Closes the output file
    void Execute() final {
        sLOG << "closing file" << path_out_;
        writer_.Close();
    }

    void Dispose() final { }

    std::string ToString() final {
        return "[WriteNode] Id:" + result_file_.ToString();
    }

protected:
    class OStreamSink : public data::BlockSink
    {
    public:
        explicit OStreamSink(const std::string& file)
            : outstream_(file) { }

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

    //! Path of the output file.
    std::string path_out_;

    //! BlockSink which writes to an actual file
    OStreamSink sink_ { path_out_ };

    //! BlockWriter to sink.
    data::BlockWriterNoVerify writer_ { &sink_ };
};

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::WriteBinary(
    const std::string& filepath) const {

    using WriteResultNode = WriteBinaryNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("WriteBinary", NodeType::ACTION);

    auto shared_node =
        std::make_shared<WriteResultNode>(*this, filepath, stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_WRITE_BINARY_HEADER

/******************************************************************************/
