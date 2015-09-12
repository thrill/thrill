/*******************************************************************************
 * thrill/api/write_lines.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_WRITE_LINES_HEADER
#define THRILL_API_WRITE_LINES_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/core/stage_builder.hpp>
#include <thrill/data/file.hpp>

#include <fstream>
#include <string>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIARef>
class WriteLinesNode : public ActionNode
{
    static const bool debug = false;

public:
    using Super = ActionNode;
    using Super::context_;

    WriteLinesNode(const ParentDIARef& parent,
                   const std::string& path_out,
                   StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, stats_node),
          path_out_(path_out),
          file_(path_out_),
          temp_file_(context_.GetFile()),
          writer_(&temp_file_)
    {
        sLOG << "Creating write node.";

        auto pre_op_fn = [=](const ValueType& input) {
                             PreOp(input);
                         };
        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    void PreOp(const ValueType& input) {
        writer_(input);
        size_ += input.size() + 1;
        stats_total_elements_++;
    }

    //! Closes the output file
    void Execute() override {
        writer_.Close();

        STAT(context_) << "NodeType" << "WriteLines"
                       << "TotalBytes" << size_
                       << "TotalLines" << stats_total_elements_;

        // (Portable) allocation of output file, setting individual file pointers.
        size_t zero = 0;
        size_t prefix_elem = context_.flow_control_channel().ExPrefixSum(size_, zero);
        if (context_.my_rank() == context_.num_workers() - 1) {
            file_.seekp(prefix_elem + size_ - 1);
            file_.put('\0');
        }
        file_.seekp(prefix_elem);
        context_.Barrier();

        data::File::ConsumeReader reader = temp_file_.GetConsumeReader();

        for (size_t i = 0; i < temp_file_.num_items(); ++i) {
            file_ << reader.Next<ValueType>() << "\n";
        }

        file_.close();
    }

    void Dispose() override { }

private:
    //! Path of the output file.
    std::string path_out_;

    //! File to write to
    std::ofstream file_;

    //! Local file size
    size_t size_ = 0;

    //! Temporary File for splitting correctly?
    data::File temp_file_;

    //! File writer used.
    data::File::Writer writer_;

    size_t stats_total_elements_ = 0;
};

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::WriteLines(
    const std::string& filepath) const {
    assert(IsValid());

    static_assert(std::is_same<ValueType, std::string>::value,
                  "WriteLines needs an std::string as input parameter");

    using WriteResultNode = WriteLinesNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("WriteLines", DIANodeType::ACTION);
    auto shared_node =
        std::make_shared<WriteResultNode>(*this,
                                          filepath,
                                          stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_WRITE_LINES_HEADER

/******************************************************************************/
