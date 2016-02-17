/*******************************************************************************
 * thrill/api/min.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_MIN_HEADER
#define THRILL_API_MIN_HEADER

#include <thrill/api/sum.hpp>
#include <thrill/common/functional.hpp>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename Stack>
template <typename MinFunction>
auto DIA<ValueType, Stack>::Min(
    const MinFunction &min_function, const ValueType &initial_value) const {
    assert(IsValid());

    using MinNode = api::SumNode<DIA, MinFunction>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<MinFunction>::template arg<0> >::value,
        "MinFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<MinFunction>::template arg<1> >::value,
        "MinFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename FunctionTraits<MinFunction>::result_type,
            ValueType>::value,
        "MinFunction has the wrong input type");

    StatsNode* stats_node = AddChildStatsNode("Min", DIANodeType::ACTION);
    auto node = std::make_shared<MinNode>(
        *this, min_function, initial_value, stats_node);

    node->RunScope();

    return node->result();
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_MIN_HEADER

/******************************************************************************/
