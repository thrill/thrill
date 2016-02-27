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

#include <string>
#include <type_traits>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ParentDIA, typename SumFunction>
class SumNode final : public ActionNode
{
    static constexpr bool debug = false;

    using Super = ActionNode;
    using Super::context_;

    //! input and result type is the parent's output value type.
    using ValueType = typename ParentDIA::ValueType;

public:
    SumNode(const ParentDIA& parent,
            const char* label,
            const SumFunction& sum_function,
            const ValueType& initial_value)
        : ActionNode(parent.ctx(), label, { parent.id() }, { parent.node() }),
          sum_function_(sum_function),
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
            sum_ = sum_function_(sum_, input);
        }
    }

    //! Executes the sum operation.
    void Execute() final {
        // process the reduce
        sum_ = context_.net.AllReduce(sum_, sum_function_);
    }

    //! Returns result of global sum.
    ValueType result() const {
        return sum_;
    }

private:
    //! The sum function which is applied to two values.
    SumFunction sum_function_;
    //! Local/global sum to be used in all reduce operation.
    ValueType sum_;
    //! indicate that sum_ is the default constructed first value. Worker 0's
    //! value is already set to initial_value.
    bool first_;
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

    auto node = std::make_shared<SumNode>(
        *this, "Sum", sum_function, initial_value);

    node->RunScope();

    return node->result();
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SUM_HEADER

/******************************************************************************/
