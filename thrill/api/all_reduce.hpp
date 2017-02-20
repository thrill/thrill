/*******************************************************************************
 * thrill/api/all_reduce.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_ALL_REDUCE_HEADER
#define THRILL_API_ALL_REDUCE_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>

#include <type_traits>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType, typename ReduceFunction>
class AllReduceNode final : public ActionResultNode<ValueType>
{
    static constexpr bool debug = false;

    using Super = ActionResultNode<ValueType>;
    using Super::context_;

public:
    template <typename ParentDIA>
    AllReduceNode(const ParentDIA& parent,
                  const char* label,
                  const ValueType& initial_value,
                  const ReduceFunction& reduce_function = ReduceFunction())
        : Super(parent.ctx(), label, { parent.id() }, { parent.node() }),
          reduce_function_(reduce_function),
          sum_(initial_value),
          first_(parent.ctx().my_rank() != 0)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [this](const ValueType& input) {
                             PreOp(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    void PreOp(const ValueType& input) {
        if (THRILL_UNLIKELY(first_)) {
            first_ = false;
            sum_ = input;
        }
        else {
            sum_ = reduce_function_(sum_, input);
        }
    }

    //! Executes the sum operation.
    void Execute() final {
        // process the reduce
        sum_ = context_.net.AllReduce(sum_, reduce_function_);
    }

    //! Returns result of global sum.
    const ValueType& result() const final {
        return sum_;
    }

private:
    //! The sum function which is applied to two values.
    ReduceFunction reduce_function_;
    //! Local/global sum to be used in all reduce operation.
    ValueType sum_;
    //! indicate that sum_ is the default constructed first value. Worker 0's
    //! value is already set to initial_value.
    bool first_;
};

template <typename ValueType, typename Stack>
template <typename ReduceFunction>
ValueType DIA<ValueType, Stack>::AllReduce(
    const ReduceFunction& sum_function, const ValueType& initial_value) const {
    assert(IsValid());

    using AllReduceNode = api::AllReduceNode<ValueType, ReduceFunction>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<ReduceFunction>::template arg<0> >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<ReduceFunction>::template arg<1> >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename FunctionTraits<ReduceFunction>::result_type,
            ValueType>::value,
        "ReduceFunction has the wrong input type");

    auto node = tlx::make_counting<AllReduceNode>(
        *this, "AllReduce", initial_value, sum_function);

    node->RunScope();

    return node->result();
}

template <typename ValueType, typename Stack>
template <typename ReduceFunction>
Future<ValueType> DIA<ValueType, Stack>::AllReduceFuture(
    const ReduceFunction& sum_function, const ValueType& initial_value) const {
    assert(IsValid());

    using AllReduceNode = api::AllReduceNode<ValueType, ReduceFunction>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<ReduceFunction>::template arg<0> >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<ReduceFunction>::template arg<1> >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename FunctionTraits<ReduceFunction>::result_type,
            ValueType>::value,
        "ReduceFunction has the wrong input type");

    auto node = tlx::make_counting<AllReduceNode>(
        *this, "AllReduce", initial_value, sum_function);

    return Future<ValueType>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_ALL_REDUCE_HEADER

/******************************************************************************/
