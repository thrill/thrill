/*******************************************************************************
 * c7a/api/write.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_WRITE_HEADER
#define C7A_API_WRITE_HEADER

#include <c7a/api/action_node.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/core/stage_builder.hpp>

#include <fstream>
#include <string>

namespace c7a {
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
        parent.node()->RegisterChild(lop_chain); 
    }

    void PreOp(ValueType input) {
		file_ << input;
    }

    //! Closes the output file
    void Execute() override {
        sLOG << "closing file" << path_out_;
		file_.close();
    }

    void Dispose() override { }

    /*!
     * Returns "[WriteNode]" and its id as a string.
     * \return "[WriteNode]"
     */
    std::string ToString() override {
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

    using WriteResultNode = WriteLinesManyNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("WriteLinesMany", "Action");
    auto shared_node =
        std::make_shared<WriteResultNode>(*this,
                                          filepath,
                                          stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_WRITE_HEADER

/******************************************************************************/
