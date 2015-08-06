/*******************************************************************************
 * c7a/api/size.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_SIZE_HEADER
#define C7A_API_SIZE_HEADER

#include <c7a/api/action_node.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/core/stage_builder.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>
#include <c7a/net/group.hpp>

#include <string>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIARef>
class SizeNode : public ActionNode
{
    static const bool debug = false;

    using Super = ActionNode;
    using Super::context_;
    using Super::parents;
    using Super::result_file_;

public:
    SizeNode(const ParentDIARef& parent,
             StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, "Size", stats_node)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](const ValueType&) { ++local_size_; };

        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain);
    }

    //! Executes the size operation.
    void Execute() final {
        MainOp();
    }

    void Dispose() final { }

    /*!
     * Returns result of global size.
     * \return result
     */
    auto result() {
        return global_size_;
    }

    /*!
     * Returns "[SizeNode]" as a string.
     * \return "[SizeNode]"
     */
    std::string ToString() final {
        return "[SizeNode] Id:" + result_file_.ToString();
    }

private:
    // Local size to be used.
    size_t local_size_ = 0;
    // Global size resulting from all reduce.
    size_t global_size_ = 0;

    void PreOp() { }

    void MainOp() {
        // get the number of elements that are stored on this worker
        LOG << "MainOp processing, sum: " << local_size_;
        net::FlowControlChannel& channel = context_.flow_control_channel();

        // process the reduce, default argument is SumOp.
        global_size_ = channel.AllReduce(local_size_);
    }

    void PostOp() { }
};

template <typename ValueType, typename Stack>
size_t DIARef<ValueType, Stack>::Size() const {

    using SizeResultNode = SizeNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("Size", "Action");
    auto shared_node
        = std::make_shared<SizeResultNode>(*this, stats_node);

    core::StageBuilder().RunScope(shared_node.get());
    return shared_node.get()->result();
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_SIZE_HEADER

/******************************************************************************/
