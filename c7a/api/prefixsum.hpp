/*******************************************************************************
 * c7a/api/prefixsum.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_PREFIXSUM_HEADER
#define C7A_API_PREFIXSUM_HEADER

#include <c7a/api/dia.hpp>
#include <c7a/api/dop_node.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/data/file.hpp>
#include <c7a/net/collective_communication.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>

#include <string>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentStack, typename SumFunction>
class PrefixSumNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;
    using Super::result_file_;

    using ParentInput = typename ParentStack::Input;

public:
    PrefixSumNode(Context& ctx,
                  const std::shared_ptr<DIANode<ParentInput> >& parent,
                  const ParentStack& parent_stack,
                  SumFunction sum_function,
                  ValueType neutral_element)
        : DOpNode<ValueType>(ctx, { parent }, "PrefixSum"),
          sum_function_(sum_function),
          local_sum_(neutral_element),
          neutral_element_(neutral_element)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](const ValueType& input) {
                             PreOp(input);
                         };

        auto lop_chain = parent_stack.push(pre_op_fn).emit();

        parent->RegisterChild(lop_chain);
    }

    virtual ~PrefixSumNode() { }

    //! Executes the sum operation.
    void Execute() override {
        this->StartExecutionTimer();
        MainOp();
        this->StopExecutionTimer();
    }

    /*!
     * Produces an 'empty' function stack, which only contains the identity
     * emitter function.
     *
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
        return "[PrefixSumNode] Id:" + result_file_.ToString();
    }

private:
    //! The sum function which is applied to two elements.
    SumFunction sum_function_;
    //! Local sum to be used in all reduce operation.
    ValueType local_sum_;
    //! Neutral element.
    ValueType neutral_element_;

    //! Local data file
    data::File file_;
    //! Data writer to local file (only active in PreOp).
    data::File::Writer writer_ = file_.GetWriter();

    //! PreOp: compute local prefixsum and store items.
    void PreOp(const ValueType& input) {
        LOG << "Input: " << input;
        local_sum_ = sum_function_(local_sum_, input);
        writer_(input);
    }

    void MainOp() {
        writer_.Close();

        LOG << "MainOp processing";
        net::FlowControlChannel& channel = context_.flow_control_channel();

        ValueType sum = channel.PrefixSum(local_sum_, sum_function_, false);

        if (context_.rank() == 0) {
            sum = neutral_element_;
        }

        data::File::Reader reader = file_.GetReader();

        for (size_t i = 0; i < file_.NumItems(); ++i) {
            sum = sum_function_(sum, reader.Next<ValueType>());
            for (auto func : DIANode<ValueType>::callbacks_) {
                func(sum);
            }
        }
    }

    void PostOp() { }
};

template <typename ValueType, typename Stack>
template <typename SumFunction>
auto DIARef<ValueType, Stack>::PrefixSum(
    const SumFunction &sum_function, ValueType neutral_element) const {

    using SumResultNode
              = PrefixSumNode<ValueType, Stack, SumFunction>;

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

    auto sum_stack = shared_node->ProduceStack();

    return DIARef<ValueType, decltype(sum_stack)>
               (shared_node, sum_stack);
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_PREFIXSUM_HEADER

/******************************************************************************/
