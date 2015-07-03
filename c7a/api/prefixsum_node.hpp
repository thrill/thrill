/*******************************************************************************
 * c7a/api/prefixsum_node.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_PREFIXSUM_NODE_HEADER
#define C7A_API_PREFIXSUM_NODE_HEADER

#include "function_stack.hpp"
#include "dia.hpp"
#include <c7a/net/group.hpp>
#include <c7a/net/collective_communication.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>
#include <c7a/common/logger.hpp>

namespace c7a {
namespace api {

template <typename Input, typename Output, typename Stack, typename SumFunction>
class PrefixSumNode : public DOpNode<Output>
{
    static const bool debug = false;

    using Super = DOpNode<Input>;
    using Super::context_;
    using Super::data_id_;
    using SumArg0 = typename FunctionTraits<SumFunction>::template arg<0>;

public:
    PrefixSumNode(Context& ctx,
            DIANode<Input>* parent,
            Stack& stack,
            SumFunction sum_function)
        : DOpNode<Output>(ctx, { parent }),
          stack_(stack),
          sum_function_(sum_function)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](Input input) {
                             PreOp(input);
                         };

        auto lop_chain = stack_.push(pre_op_fn).emit();

        parent->RegisterChild(lop_chain);
    }

    virtual ~PrefixSumNode() { }

    //! Executes the sum operation.
    void execute() override {
        MainOp();
    }

    /*!
     * Produces an 'empty' function stack, which only contains the identity emitter function.
     * \return Empty function stack
     */
    auto ProduceStack() {
        // Hook Identity
        auto id_fn = [=](Input t, auto emit_func) {
                         return emit_func(t);
                     };

        FunctionStack<> stack;
        return stack.push(id_fn);
    }

    /*!
     * Returns "[PrefixSumNode]" as a string.
     * \return "[PrefixSumNode]"
     */
    std::string ToString() override {
        return "[PrefixSumNode] Id:" + data_id_.ToString();
    }

private:
    //! Local stack.
    Stack stack_;
    //! The sum function which is applied to two elements.
    SumFunction sum_function_;
    //! Local sum to be used in all reduce operation.
    Input local_sum = 0;
    //! Local data
    std::vector<Input> data_;

    void PreOp(SumArg0 input) {
        LOG << "Input: " << input;
        local_sum = sum_function_(local_sum, input);
        data_.push_back(input);
    }

    void MainOp() {
        LOG << "MainOp processing";
        net::FlowControlChannel& channel = context_.get_flow_control_channel();

        Input prefix_sum = channel.PrefixSum(local_sum, sum_function_, false);

        for (size_t i = 0; i < data_.size(); i++) {
            prefix_sum = sum_function_(prefix_sum, data_[i]);
            for (auto func : DIANode<Output>::callbacks_) {
                func(prefix_sum);
            }
         }
    }

    void PostOp() { }
};

template <typename T, typename Stack>
template <typename SumFunction>
auto DIARef<T, Stack>::PrefixSum(const SumFunction &sum_function) {
    using SumResult
              = typename FunctionTraits<SumFunction>::result_type;
    using SumArgument0
              = typename FunctionTraits<SumFunction>::template arg<0>;

    using SumResultNode
              = PrefixSumNode<SumArgument0, SumResult,
                        decltype(local_stack_), SumFunction>;

    auto shared_node
        = std::make_shared<SumResultNode>(node_->get_context(),
                                          node_.get(),
                                          local_stack_,
                                          sum_function);


    auto sum_stack = shared_node->ProduceStack();

    return DIARef<SumResult, decltype(sum_stack)>
               (std::move(shared_node), sum_stack);
}
}

} // namespace api

#endif // !C7A_API_PREFIXSUM_NODE_HEADER

/******************************************************************************/
