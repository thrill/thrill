/*******************************************************************************
 * thrill/api/ex_prefix_sum.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_EX_PREFIX_SUM_HEADER
#define THRILL_API_EX_PREFIX_SUM_HEADER

#include <thrill/api/prefix_sum.hpp>
#include <thrill/common/functional.hpp>

namespace thrill {
namespace api {

template <typename ValueType, typename Stack>
template <typename SumFunction>
auto DIA<ValueType, Stack>::ExPrefixSum(
    const SumFunction& sum_function, const ValueType& initial_element) const {
    assert(IsValid());

    using PrefixSumNode = api::PrefixSumNode<
        ValueType, SumFunction, /* Inclusive */ false>;

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

    auto node = tlx::make_counting<PrefixSumNode>(
        *this, "ExPrefixSum", sum_function, initial_element);

    return DIA<ValueType>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_EX_PREFIX_SUM_HEADER

/******************************************************************************/
