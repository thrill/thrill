/*******************************************************************************
 * c7a/api/sum.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_SUM_HEADER
#define C7A_API_SUM_HEADER

#include <c7a/api/action_node.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/core/stage_builder.hpp>
#include <c7a/net/collective_communication.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>
#include <c7a/net/group.hpp>

#include <string>
#include <type_traits>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIARef, typename SumFunction>
class SumNode : public ActionNode
{
    static const bool debug = false;

    using Super = ActionNode;
    using Super::context_;
    using SumArg0 = ValueType;

public:
    SumNode(const ParentDIARef& parent,
            SumFunction sum_function,
            ValueType initial_value,
            StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, "Sum", stats_node),
          sum_function_(sum_function),
          local_sum_(initial_value)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](ValueType input) {
                             PreOp(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain);
    }

    //! Executes the sum operation.
    void Execute() override {
        MainOp();
    }

    void Dispose() override { }

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
        return "[SumNode] Id:" + result_file_.ToString();
    }

private:
    //! The sum function which is applied to two values.
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
auto DIARef<ValueType, Stack>::Sum(
    const SumFunction &sum_function, ValueType initial_value) const {

    using SumResultNode
              = SumNode<ValueType, DIARef, SumFunction>;

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

    StatsNode* stats_node = AddChildStatsNode("Sum", "Action");
    auto shared_node
        = std::make_shared<SumResultNode>(*this,
                                          sum_function,
                                          initial_value,
                                          stats_node);
    core::StageBuilder().RunScope(shared_node.get());
    return shared_node.get()->result();
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_SUM_HEADER

/******************************************************************************/
