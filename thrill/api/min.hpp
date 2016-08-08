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

#include <thrill/api/all_reduce.hpp>
#include <thrill/common/functional.hpp>

namespace thrill {
namespace api {

template <typename ValueType, typename Stack>
ValueType DIA<ValueType, Stack>::Min(const ValueType& initial_value) const {
    assert(IsValid());

    using MinNode = api::AllReduceNode<ValueType, common::minimum<ValueType> >;
    auto node = common::MakeCounting<MinNode>(*this, "Min", initial_value);
    node->RunScope();
    return node->result();
}

template <typename ValueType, typename Stack>
Future<ValueType> DIA<ValueType, Stack>::Min(
    const struct FutureTag&, const ValueType& initial_value) const {
    assert(IsValid());

    using MinNode = api::AllReduceNode<ValueType, common::minimum<ValueType> >;
    auto node = common::MakeCounting<MinNode>(*this, "Min", initial_value);
    return Future<ValueType>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_MIN_HEADER

/******************************************************************************/
