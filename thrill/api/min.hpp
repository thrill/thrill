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
ValueType DIA<ValueType, Stack>::Min() const {
    assert(IsValid());

    using MinNode = api::AllReduceNode<ValueType, common::minimum<ValueType> >;
    auto node = tlx::make_counting<MinNode>(*this, "Min");
    node->RunScope();
    return node->result();
}

template <typename ValueType, typename Stack>
ValueType DIA<ValueType, Stack>::Min(const ValueType& initial_value) const {
    assert(IsValid());

    using MinNode = api::AllReduceNode<ValueType, common::minimum<ValueType> >;
    auto node = tlx::make_counting<MinNode>(
        *this, "Min", initial_value, /* with_initial_value */ true);
    node->RunScope();
    return node->result();
}

template <typename ValueType, typename Stack>
Future<ValueType> DIA<ValueType, Stack>::MinFuture() const {
    assert(IsValid());

    using MinNode = api::AllReduceNode<ValueType, common::minimum<ValueType> >;
    auto node = tlx::make_counting<MinNode>(*this, "Min");
    return Future<ValueType>(node);
}

template <typename ValueType, typename Stack>
Future<ValueType> DIA<ValueType, Stack>::MinFuture(
    const ValueType& initial_value) const {
    assert(IsValid());

    using MinNode = api::AllReduceNode<ValueType, common::minimum<ValueType> >;
    auto node = tlx::make_counting<MinNode>(
        *this, "Min", initial_value, /* with_initial_value */ true);
    return Future<ValueType>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_MIN_HEADER

/******************************************************************************/
