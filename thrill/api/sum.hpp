/*******************************************************************************
 * thrill/api/sum.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_SUM_HEADER
#define THRILL_API_SUM_HEADER

#include <thrill/api/all_reduce.hpp>
#include <thrill/common/functional.hpp>

namespace thrill {
namespace api {

template <typename ValueType, typename Stack>
template <typename SumFunction>
auto DIA<ValueType, Stack>::Sum(
    const SumFunction &sum_function, const ValueType &initial_value) const {
    assert(IsValid());

    using SumNode = api::AllReduceNode<ValueType, SumFunction>;

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

    auto node = common::MakeCounting<SumNode>(
        *this, "Sum", sum_function, initial_value);

    node->RunScope();

    return node->result();
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SUM_HEADER

/******************************************************************************/
