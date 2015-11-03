/*******************************************************************************
 * thrill/api/size.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_SIZE_HEADER
#define THRILL_API_SIZE_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/core/stage_builder.hpp>
#include <thrill/net/flow_control_channel.hpp>
#include <thrill/net/flow_control_manager.hpp>
#include <thrill/net/group.hpp>

#include <string>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ParentDIA>
class SizeNode final : public ActionNode
{
    static const bool debug = false;

    using Super = ActionNode;
    using Super::context_;

    //! input type is the parent's output value type.
    using Input = typename ParentDIA::ValueType;

public:
    SizeNode(const ParentDIA& parent,
             StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, stats_node)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](const Input&) { ++local_size_; };

        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
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
    size_t result() const {
        return global_size_;
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
size_t DIA<ValueType, Stack>::Size() const {
    assert(IsValid());

    using SizeNode = api::SizeNode<DIA>;

    StatsNode* stats_node = AddChildStatsNode("Size", DIANodeType::ACTION);
    auto shared_node
        = std::make_shared<SizeNode>(*this, stats_node);

    core::StageBuilder().RunScope(shared_node.get());
    return shared_node.get()->result();
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SIZE_HEADER

/******************************************************************************/
