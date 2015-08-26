/*******************************************************************************
 * thrill/api/write_lines_many.hpp
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
#ifndef THRILL_API_WRITE_LINES_MANY_HEADER
#define THRILL_API_WRITE_LINES_MANY_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/core/stage_builder.hpp>

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
    using Super::result_file_;
    using Super::context_;

    WriteLinesManyNode(const ParentDIARef& parent,
                       const std::string& path_out,
                       StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, "Write", stats_node),
          path_out_(path_out),
          file_(path_out_)
    {
        sLOG << "Creating write node.";

        auto pre_op_fn = [=](ValueType input) {
                             PreOp(input);
                         };
        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    void PreOp(ValueType input) {
        file_ << input << "\n";
    }

    //! Closes the output file
    void Execute() final {
        sLOG << "closing file" << path_out_;
        file_.close();
    }

    void Dispose() final { }

    /*!
     * Returns "[WriteNode]" and its id as a string.
     * \return "[WriteNode]"
     */
    std::string ToString() final {
        return "[WriteNode] Id:" + result_file_.ToString();
    }

private:
    //! Path of the output file.
    std::string path_out_;

    //! File to write to
    std::ofstream file_;
};

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::WriteLinesMany(
    const std::string& filepath) const {
    assert(IsValid());

    static_assert(std::is_same<ValueType, std::string>::value,
                  "WriteLinesMany needs an std::string as input parameter");

    using WriteResultNode = WriteLinesManyNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("WriteLinesMany", NodeType::ACTION);

    auto shared_node =
        std::make_shared<WriteResultNode>(*this,
                                          filepath,
                                          stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_WRITE_LINES_MANY_HEADER

/******************************************************************************/
