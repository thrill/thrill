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

/******************************************************************************/
// FlowControlChannel

FlowControlChannel::FlowControlChannel(
    Group& group, size_t local_id, size_t thread_count,
    common::ThreadBarrier& barrier, LocalData* shmem,
    std::atomic<size_t>& generation)
    : group_(group),
      host_rank_(group_.my_host_rank()), num_hosts_(group_.num_hosts()),
      local_id_(local_id),
      thread_count_(thread_count),
      barrier_(barrier), shmem_(shmem), generation_(generation) { }

FlowControlChannel::~FlowControlChannel() {
    sLOGC(enable_stats)
        << "FCC worker" << my_rank() << ":"
        << "prefixsum"
        << count_prefixsum_ << "in" << timer_prefixsum_
        << "broadcast"
        << count_broadcast_ << "in" << timer_broadcast_
        << "reduce"
        << count_reduce_ << "in" << timer_reduce_
        << "allreduce"
        << count_allreduce_ << "in" << timer_allreduce_
        << "predecessor"
        << count_predecessor_ << "in" << timer_predecessor_
        << "barrier"
        << count_barrier_ << "in" << timer_barrier_
        << "communication"
        << timer_communication_;
}

void FlowControlChannel::Barrier() {
    RunTimer run_timer(timer_barrier_);
    if (enable_stats || debug) ++count_barrier_;

    LOG << "FCC::Barrier() ENTER count=" << count_barrier_;

    barrier_.Await(
        [&]() {
            RunTimer net_timer(timer_communication_);

            LOG << "FCC::Barrier() COMMUNICATE BEGIN"
                << " count=" << count_barrier_;

            // Global all reduce
            size_t i = 0;
            group_.AllReduce(i);

            LOG << "FCC::Barrier() COMMUNICATE END"
                << " count=" << count_barrier_;
        });

    LOG << "FCC::Barrier() EXIT count=" << count_barrier_;
}

void FlowControlChannel::LocalBarrier() {
    barrier_.Await();
}

/******************************************************************************/
// template instantiations

template size_t FlowControlChannel::PrefixSum(
    const size_t&, const size_t&, const std::plus<size_t>&, bool);

template std::array<size_t, 2> FlowControlChannel::PrefixSum(
    const std::array<size_t, 2>&, const std::array<size_t, 2>&,
    const common::ComponentSum<std::array<size_t, 2> >&, bool);
template std::array<size_t, 3> FlowControlChannel::PrefixSum(
    const std::array<size_t, 3>&, const std::array<size_t, 3>&,
    const common::ComponentSum<std::array<size_t, 3> >&, bool);
template std::array<size_t, 4> FlowControlChannel::PrefixSum(
    const std::array<size_t, 4>&, const std::array<size_t, 4>&,
    const common::ComponentSum<std::array<size_t, 4> >&, bool);

template size_t FlowControlChannel::ExPrefixSumTotal(
    size_t&, const size_t&, const std::plus<size_t>&);

template std::array<size_t, 2> FlowControlChannel::ExPrefixSumTotal(
    std::array<size_t, 2>&, const std::array<size_t, 2>&,
    const common::ComponentSum<std::array<size_t, 2> >&);
template std::array<size_t, 3> FlowControlChannel::ExPrefixSumTotal(
    std::array<size_t, 3>&, const std::array<size_t, 3>&,
    const common::ComponentSum<std::array<size_t, 3> >&);
template std::array<size_t, 4> FlowControlChannel::ExPrefixSumTotal(
    std::array<size_t, 4>&, const std::array<size_t, 4>&,
    const common::ComponentSum<std::array<size_t, 4> >&);

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
