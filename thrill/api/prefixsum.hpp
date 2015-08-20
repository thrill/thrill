/*******************************************************************************
 * thrill/api/prefixsum.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_PREFIXSUM_HEADER
#define THRILL_API_PREFIXSUM_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>
#include <thrill/net/flow_control_channel.hpp>
#include <thrill/net/flow_control_manager.hpp>

#include <string>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIARef, typename SumFunction>
class PrefixSumNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;
    using Super::result_file_;

public:
    PrefixSumNode(const ParentDIARef& parent,
                  SumFunction sum_function,
                  ValueType neutral_element,
                  StatsNode* stats_node)
        : DOpNode<ValueType>(parent.ctx(), { parent.node() }, "PrefixSum", stats_node),
          sum_function_(sum_function),
          local_sum_(neutral_element),
          neutral_element_(neutral_element)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](const ValueType& input) {
                             PreOp(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    virtual ~PrefixSumNode() { }

    //! Executes the sum operation.
    void Execute() final {
        MainOp();
    }

    void PushData() final {
        data::File::Reader reader = file_.GetReader();

        ValueType sum = local_sum_;

        for (size_t i = 0; i < file_.NumItems(); ++i) {
            sum = sum_function_(sum, reader.Next<ValueType>());
            for (auto func : DIANode<ValueType>::callbacks_) {
                func(sum);
            }
        }
    }

    void Dispose() final { }

    /*!
     * Produces an 'empty' function stack, which only contains the identity
     * emitter function.
     *
     * \return Empty function stack
     */
    auto ProduceStack() {
        return FunctionStack<ValueType>();
    }

    /*!
     * Returns "[PrefixSumNode]" as a string.
     * \return "[PrefixSumNode]"
     */
    std::string ToString() final {
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

        if (context_.my_rank() == 0) {
            sum = neutral_element_;
        }

        local_sum_ = sum;
    }

    void PostOp() { }
};

template <typename ValueType, typename Stack>
template <typename SumFunction>
auto DIARef<ValueType, Stack>::PrefixSum(
    const SumFunction &sum_function, ValueType neutral_element) const {

    using SumResultNode
              = PrefixSumNode<ValueType, DIARef, SumFunction>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<SumFunction>::template arg<0>
            >::value,
        "SumFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<SumFunction>::template arg<1> >::value,
        "SumFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename FunctionTraits<SumFunction>::result_type,
            ValueType>::value,
        "SumFunction has the wrong input type");

    StatsNode* stats_node = AddChildStatsNode("PrefixSum", NodeType::DOP);
    auto shared_node
        = std::make_shared<SumResultNode>(*this,
                                          sum_function,
                                          neutral_element,
                                          stats_node);

    auto sum_stack = shared_node->ProduceStack();

    return DIARef<ValueType, decltype(sum_stack)>(
        shared_node,
        sum_stack,
        { stats_node });
}

//! \}

} // namespace api
} // namespace c7a

#endif // !THRILL_API_PREFIXSUM_HEADER

/******************************************************************************/
