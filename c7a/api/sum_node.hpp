/*******************************************************************************
 * c7a/api/sum_node.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_SUM_NODE_HEADER
#define C7A_API_SUM_NODE_HEADER

#include <c7a/api/action_node.hpp>
#include <c7a/api/function_stack.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/net/group.hpp>
#include <c7a/net/collective_communication.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>

#include <type_traits>

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
            std::shared_ptr<DIANode<ParentInput> > parent,
            ParentStack& parent_stack,
            SumFunction sum_function,
            ValueType neutral_element)
        : ActionNode(ctx, { parent }),
          sum_function_(sum_function),
          local_sum_(neutral_element)
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
        return global_sum_;
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
    ValueType local_sum_;
    // Global sum resulting from all reduce.
    ValueType global_sum_;

    void PreOp(ValueType input) {
        LOG << "PreOp: " << input;
        local_sum_ = sum_function_(local_sum_, input);
    }

    void MainOp() {
        LOG << "MainOp processing";
        net::FlowControlChannel& channel = context_.flow_control_channel();

        // process the reduce
        global_sum_ = channel.AllReduce(local_sum_, sum_function_);
    }

    void PostOp() { }
};

template <typename ValueType, typename Stack>
template <typename SumFunction>
auto DIARef<ValueType, Stack>::Sum(const SumFunction &sum_function,
                                   ValueType neutral_element) {
    using SumResultNode
              = SumNode<ValueType, Stack, SumFunction>;

    static_assert(
        std::is_same<
            typename common::FunctionTraits<SumFunction>::template arg<0>,
            ValueType>::value ||
        std::is_same<SumFunction, common::SumOp<ValueType> >::value,
        "SumFunction has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<SumFunction>::template arg<1>,
            ValueType>::value ||
        std::is_same<SumFunction, common::SumOp<ValueType> >::value,
        "SumFunction has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<SumFunction>::result_type,
            ValueType>::value ||
        std::is_same<SumFunction, common::SumOp<ValueType> >::value,
        "SumFunction has the wrong input type");

    auto shared_node
        = std::make_shared<SumResultNode>(node_->context(),
                                          node_,
                                          stack_,
                                          sum_function,
                                          neutral_element);

    core::StageBuilder().RunScope(shared_node.get());
    return shared_node.get()->result();
}

} // namespace api
} // namespace c7a

#endif // !C7A_API_SUM_NODE_HEADER

/******************************************************************************/
