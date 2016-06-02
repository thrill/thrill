/*******************************************************************************
 * thrill/net/flow_control_channel.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/net/flow_control_channel.hpp>

#include <functional>

namespace thrill {
namespace net {

template size_t FlowControlChannel::PrefixSum(
    const size_t&, const size_t&, const std::plus<size_t>&, bool);

template std::array<size_t, 2> FlowControlChannel::PrefixSum(
    const std::array<size_t, 2>&, const std::array<size_t, 2>&,
    const common::ComponentSum<std::array<size_t, 2> >&, bool);

template size_t FlowControlChannel::Broadcast(const size_t&, size_t);

template std::array<size_t, 2> FlowControlChannel::Broadcast(
    const std::array<size_t, 2>&, size_t);
template std::array<size_t, 3> FlowControlChannel::Broadcast(
    const std::array<size_t, 3>&, size_t);
template std::array<size_t, 4> FlowControlChannel::Broadcast(
    const std::array<size_t, 4>&, size_t);

template size_t FlowControlChannel::AllReduce(
    const size_t&, const std::plus<size_t>&);

} // namespace net
} // namespace thrill

/******************************************************************************/
