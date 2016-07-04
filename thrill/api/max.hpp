/*******************************************************************************
 * thrill/api/max.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_MAX_HEADER
#define THRILL_API_MAX_HEADER

#include <thrill/api/all_reduce.hpp>
#include <thrill/common/functional.hpp>

namespace thrill {
namespace api {

template <typename ValueType, typename Stack>
template <typename MaxFunction>
auto DIA<ValueType, Stack>::Max(
    const MaxFunction& max_function, const ValueType& initial_value) const {
    assert(IsValid());

    using MaxNode = api::AllReduceNode<ValueType, MaxFunction>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<MaxFunction>::template arg<0> >::value,
        "MaxFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<MaxFunction>::template arg<1> >::value,
        "MaxFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename FunctionTraits<MaxFunction>::result_type,
            ValueType>::value,
        "MaxFunction has the wrong input type");

    auto node = common::MakeCounting<MaxNode>(
        *this, "Max", max_function, initial_value);

    node->RunScope();

    return node->result();
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_MAX_HEADER

/******************************************************************************/
