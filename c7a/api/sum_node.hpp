/*******************************************************************************
 * c7a/api/sum_node.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_SUM_NODE_HEADER
#define C7A_API_SUM_NODE_HEADER

#include "action_node.hpp"
#include "function_stack.hpp"
#include "dia.hpp"
#include <c7a/net/group.hpp>
#include <c7a/net/collective_communication.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>

namespace c7a {
namespace api {

template <typename ValueType, typename ParentStack, typename SumFunction>
class SumNode : public ActionNode
{
    static const bool debug = false;

    using Super = ActionNode;
    using Super::context_;
    using Super::data_id_;
    using SumArg0 = ValueType;

    using ParentInput = typename ParentStack::Input;

public:
    SumNode(Context& ctx,
            DIANode<ParentInput>* parent,
            ParentStack& parent_stack,
            SumFunction sum_function)
        : ActionNode(ctx, { parent }),
          sum_function_(sum_function)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](ValueType input) {
                             PreOp(input);
                         };

        auto lop_chain = parent_stack.push(pre_op_fn).emit();
        parent->RegisterChild(lop_chain);
    }

    virtual ~SumNode() { }

    //! Executes the sum operation.
    void Execute() override {
        MainOp();
    }

    /*!
     * Returns result of global sum.
     * \return result
     */
    auto result() {
        return global_sum;
    }

    /*!
     * Returns "[SumNode]" as a string.
     * \return "[SumNode]"
     */
    std::string ToString() override {
        return "[SumNode] Id:" + data_id_.ToString();
    }

private:
    //! The sum function which is applied to two elements.
    SumFunction sum_function_;
    // Local sum to be used in all reduce operation.
    ValueType local_sum = 0;
    // Global sum resulting from all reduce.
    ValueType global_sum = 0;

    void PreOp(ValueType input) {
        LOG << "PreOp: " << input;
        local_sum = sum_function_(local_sum, input);
    }

    void MainOp() {
        LOG << "MainOp processing";
        net::FlowControlChannel& channel = context_.get_flow_control_channel();

        // process the reduce
        global_sum = channel.AllReduce(local_sum, sum_function_);
    }

    void PostOp() { }
};

template <typename ValueType, typename Stack>
template <typename SumFunction>
auto DIARef<ValueType, Stack>::Sum(const SumFunction &sum_function) {
    using SumResultNode
              = SumNode<ValueType, Stack, SumFunction>;

    auto shared_node
        = std::make_shared<SumResultNode>(node_->get_context(),
                                          node_.get(),
                                          local_stack_,
                                          sum_function);

    core::StageBuilder().RunScope(shared_node.get());
    return shared_node.get()->result();
}

} // namespace api
} // namespace c7a

#endif // !C7A_API_SUM_NODE_HEADER

/******************************************************************************/
