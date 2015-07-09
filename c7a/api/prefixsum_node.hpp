/*******************************************************************************
 * c7a/api/prefixsum_node.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_PREFIXSUM_NODE_HEADER
#define C7A_API_PREFIXSUM_NODE_HEADER

#include "function_stack.hpp"
#include "dia.hpp"
#include "context.hpp"
#include "function_stack.hpp"
#include <c7a/net/group.hpp>
#include <c7a/net/collective_communication.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>
#include <c7a/common/logger.hpp>

namespace c7a {
namespace api {

template <typename ParentType, typename ValueType, typename Stack, typename SumFunction>
class PrefixSumNode : public DOpNode<ParentType>
{
    static const bool debug = false;

    using Super = DOpNode<ParentType>;
    using Super::context_;
    using Super::data_id_;

public:

    PrefixSumNode(Context& ctx,
				  DIANode<ParentType>* parent,
				  Stack& stack,
				  SumFunction sum_function,
				  ValueType neutral_element)
        : DOpNode<ValueType>(ctx, { parent }),
		  stack_(stack),
          sum_function_(sum_function),
		  local_sum_(neutral_element),
		  neutral_element_(neutral_element)
		{
			// Hook PreOp(s)
			auto pre_op_fn = [=](ValueType input) {
				PreOp(input);
			};

			auto lop_chain = stack_.push(pre_op_fn).emit();

			parent->RegisterChild(lop_chain);
		}

    virtual ~PrefixSumNode() { }

    //! Executes the sum operation.
    void Execute() override {
        MainOp();
    }

    /*!
     * Produces an 'empty' function stack, which only contains the identity emitter function.
     * \return Empty function stack
     */
    auto ProduceStack() {
        // Hook Identity
        auto id_fn = [=](ValueType t, auto emit_func) {
                         return emit_func(t);
                     };

		
        return MakeFunctionStack<ValueType>(id_fn);
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
    ValueType local_sum_;
	//! Neutral element.
	ValueType neutral_element_;
	//! Local data
	std::vector<ValueType> data_;

    void PreOp(ValueType input) {
		LOG << "Input: " << input;
        local_sum_ = sum_function_(local_sum_, input);
		data_.push_back(input);
    }

    void MainOp() {
        LOG << "MainOp processing";
        net::FlowControlChannel& channel = context_.get_flow_control_channel();
		
		ValueType prefix_sum = channel.PrefixSum(local_sum_, sum_function_, false);

		if(context_.rank() == 0) {
			prefix_sum = neutral_element_;
		}

		for (size_t i = 0; i < data_.size(); i++) {
			prefix_sum = sum_function_(prefix_sum, data_[i]);
			for (auto func : DIANode<ValueType>::callbacks_) {
				func(prefix_sum);
			}
	   }
    }

    void PostOp() { }
};

template <typename ParentType, typename ValueType, typename Stack>
template <typename SumFunction>
auto DIARef<ParentType, ValueType, Stack>::PrefixSum(const SumFunction &sum_function, ValueType neutral_element) {

    using SumResultNode
              = PrefixSumNode<ParentType, ValueType,
                        decltype(local_stack_), SumFunction>;

    auto shared_node
        = std::make_shared<SumResultNode>(node_->get_context(),
                                          node_.get(),
                                          local_stack_,
                                          sum_function,
										  neutral_element
			);

    auto sum_stack = shared_node->ProduceStack();

    return DIARef<ValueType, ValueType, decltype(sum_stack)>
               (std::move(shared_node), sum_stack);
}
}
} // namespace api

#endif // !C7A_API_PREFIXSUM_NODE_HEADER

/******************************************************************************/
