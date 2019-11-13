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
ValueType DIA<ValueType, Stack>::Max() const {
    assert(IsValid());

    using MaxNode = api::AllReduceNode<ValueType, common::maximum<ValueType> >;
    auto node = tlx::make_counting<MaxNode>(*this, "Max");
    node->RunScope();
    return node->result();
}

template <typename ValueType, typename Stack>
ValueType DIA<ValueType, Stack>::Max(const ValueType& initial_value) const {
    assert(IsValid());

    using MaxNode = api::AllReduceNode<ValueType, common::maximum<ValueType> >;
    auto node = tlx::make_counting<MaxNode>(
        *this, "Max", initial_value, /* with_initial_value */ true);
    node->RunScope();
    return node->result();
}

template <typename ValueType, typename Stack>
Future<ValueType> DIA<ValueType, Stack>::MaxFuture() const {
    assert(IsValid());

    using MaxNode = api::AllReduceNode<ValueType, common::maximum<ValueType> >;
    auto node = tlx::make_counting<MaxNode>(*this, "Max");
    return Future<ValueType>(node);
}

template <typename ValueType, typename Stack>
Future<ValueType> DIA<ValueType, Stack>::MaxFuture(
    const ValueType& initial_value) const {
    assert(IsValid());

    using MaxNode = api::AllReduceNode<ValueType, common::maximum<ValueType> >;
    auto node = tlx::make_counting<MaxNode>(
        *this, "Max", initial_value, /* with_initial_value */ true);
    return Future<ValueType>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_MAX_HEADER

/******************************************************************************/
