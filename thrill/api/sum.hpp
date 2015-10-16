/*******************************************************************************
 * thrill/api/sum.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_SUM_HEADER
#define THRILL_API_SUM_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/core/stage_builder.hpp>
#include <thrill/net/flow_control_channel.hpp>
#include <thrill/net/flow_control_manager.hpp>
#include <thrill/net/group.hpp>

#include <string>
#include <type_traits>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ParentDIA, typename SumFunction>
class SumNode final : public ActionNode
{
    static const bool debug = false;

    using Super = ActionNode;
    using Super::context_;

    //! input and result type is the parent's output value type.
    using ValueType = typename ParentDIA::ValueType;

public:
    SumNode(const ParentDIA& parent,
            const SumFunction& sum_function,
            const ValueType& initial_value,
            StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, stats_node),
          sum_function_(sum_function),
          local_sum_(initial_value)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](const ValueType& input) {
                             PreOp(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    //! Executes the sum operation.
    void Execute() final {
        MainOp();
    }

    void Dispose() final { }

    /*!
     * Returns result of global sum.
     * \return result
     */
    ValueType result() const {
        return global_sum_;
    }

private:
    //! The sum function which is applied to two values.
    SumFunction sum_function_;
    // Local sum to be used in all reduce operation.
    ValueType local_sum_;
    // Global sum resulting from all reduce.
    ValueType global_sum_;

    void PreOp(ValueType input) {
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
auto DIA<ValueType, Stack>::Sum(
    const SumFunction &sum_function, const ValueType &initial_value) const {
    assert(IsValid());

    using SumNode = api::SumNode<DIA, SumFunction>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<SumFunction>::template arg<0> >::value,
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

    StatsNode* stats_node = AddChildStatsNode("Sum", DIANodeType::ACTION);
    auto shared_node
        = std::make_shared<SumNode>(
        *this, sum_function, initial_value, stats_node);

    core::StageBuilder().RunScope(shared_node.get());
    return shared_node.get()->result();
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SUM_HEADER

/******************************************************************************/
