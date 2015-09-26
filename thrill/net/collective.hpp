/*******************************************************************************
 * thrill/net/collective.hpp
 *
 * This file provides collective communication primitives, which are to be used
 * with net::Groups.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Robert Hangu <robert.hangu@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_COLLECTIVE_HEADER
#define THRILL_NET_COLLECTIVE_HEADER

#include <thrill/common/functional.hpp>
#include <thrill/common/math.hpp>
#include <thrill/net/group.hpp>

#include <functional>

namespace thrill {
namespace net {
namespace collective {

//! \addtogroup net Network Communication
//! \{

/******************************************************************************/
// Prefixsum Algorithms

/*!
 * Calculate for every worker his prefix sum.
 *
 * The prefix sum is the aggregation of the values of all workers with lesser
 * index, including himself, according to a summation operator. The run-time is
 * in O(log n).
 *
 * \param net The current worker onto which to apply the operation
 * \param value The value to be summed up
 * \param sum_op A custom summation operator
 */
template <typename T, typename BinarySumOp = std::plus<T> >
static inline
void PrefixSum(
    Group& net, T& value,
    BinarySumOp sum_op = BinarySumOp(), bool inclusive = true) {
    static const bool debug = false;

    bool first = true;
    // Use a copy, in case of exclusive, we have to forward
    // something that's not our result.
    T to_forward = value;

    // This is based on the pointer-doubling algorithm presented in the ParAlg
    // script, which is used for list ranking.
    for (size_t d = 1; d < net.num_hosts(); d <<= 1) {

        if (net.my_host_rank() + d < net.num_hosts()) {
            sLOG << "Host" << net.my_host_rank()
                 << ": sending to" << net.my_host_rank() + d;
            net.SendTo(net.my_host_rank() + d, to_forward);
        }

        if (net.my_host_rank() >= d) {
            T recv_value;
            net.ReceiveFrom(net.my_host_rank() - d, &recv_value);
            sLOG << "Host" << net.my_host_rank()
                 << ": receiving " << recv_value
                 << " from" << net.my_host_rank() - d;

            // Take care of order, so we don't break associativity.
            to_forward = sum_op(recv_value, to_forward);

            if (!first || inclusive) {
                value = sum_op(recv_value, value);
            }
            else {
                value = recv_value;
                first = false;
            }
        }
    }

    // set worker 0's value for exclusive prefixsums
    if (!inclusive && net.my_host_rank() == 0)
        value = T();
}

/*!
 * \brief Calculate for every worker his prefix sum. Works only for worker
 * numbers which are powers of two.
 *
 * \details The prefix sum is the aggregatation of the values of all workers
 * with lesser index, including himself, according to a summation operator. This
 * function currently only supports worker numbers which are powers of two.
 *
 * \param net The current worker onto which to apply the operation
 *
 * \param value The value to be summed up
 *
 * \param sum_op A custom summation operator
 */
template <typename T, typename BinarySumOp = std::plus<T> >
static inline
void PrefixSumHypercube(
    Group& net, T& value, BinarySumOp sum_op = BinarySumOp()) {
    T total_sum = value;

    static const bool debug = false;

    for (size_t d = 1; d < net.num_hosts(); d <<= 1)
    {
        // communication peer for this round (hypercube dimension)
        size_t peer = net.my_host_rank() ^ d;

        // Send total sum of this hypercube to worker with id = id XOR d
        if (peer < net.num_hosts()) {
            net.SendTo(peer, total_sum);
            sLOG << "PREFIX_SUM: host" << net.my_host_rank() << ": sending" << total_sum
                 << "to peer" << peer;
        }

        // Receive total sum of smaller hypercube from worker with id = id XOR d
        T recv_data;
        if (peer < net.num_hosts()) {
            net.ReceiveFrom(peer, &recv_data);
            // The order of addition is important. The total sum of the smaller
            // hypercube always comes first.
            if (net.my_host_rank() & d)
                total_sum = sum_op(recv_data, total_sum);
            else
                total_sum = sum_op(total_sum, recv_data);
            // Variable 'value' represents the prefix sum of this worker
            if (net.my_host_rank() & d)
                // The order of addition is respected the same way as above.
                value = sum_op(recv_data, value);
            sLOG << "PREFIX_SUM: host" << net.my_host_rank() << ": received" << recv_data
                 << "from peer" << peer
                 << "value =" << value;
        }
    }

    sLOG << "PREFIX_SUM: host" << net.my_host_rank()
         << ": value after prefix sum =" << value;
}

/******************************************************************************/
// Broadcast Algorithms

/*!
 * Broadcasts the value of the peer with index 0 to all the others. This is a
 * trivial broadcast from peer 0.
 *
 * \param net The current peer onto which to apply the operation
 *
 * \param value The value to be broadcast / receive into.
 */
template <typename T>
static inline
void BroadcastTrivial(Group& net, T& value) {

    if (net.my_host_rank() == 0) {
        // send value to all peers
        for (size_t p = 1; p < net.num_hosts(); ++p) {
            net.SendTo(p, value);
        }
    }
    else {
        // receive from peer 0
        net.ReceiveFrom(0, &value);
    }
}

/*!
 * Broadcasts the value of the worker with index 0 to all the others. This is a
 * binomial tree broadcast method.
 *
 * \param net The current worker onto which to apply the operation
 *
 * \param value The value to be broadcast / receive into.
 */
template <typename T>
static inline
void BroadcastBinomialTree(Group& net, T& value) {
    static const bool debug = false;

    size_t my_rank = net.my_host_rank();
    size_t r = 0, d = 1;
    // receive from predecessor
    if (my_rank > 0) {
        // our predecessor is p with the lowest one bit flipped to zero. this
        // also counts the number of rounds we have to send out messages later.
        while ((my_rank & d) == 0) d <<= 1, ++r;
        size_t from = my_rank ^ d;
        sLOG << "Broadcast: rank" << my_rank << "receiving from" << from
             << "in round" << r;
        net.ReceiveFrom(from, &value);
    }
    else {
        d = common::RoundUpToPowerOfTwo(net.num_hosts());
    }
    // send to successors
    for (d >>= 1; d > 0; d >>= 1, ++r) {
        if (my_rank + d < net.num_hosts()) {
            sLOG << "Broadcast: rank" << my_rank << "round" << r << "sending to"
                 << my_rank + d;
            net.SendTo(my_rank + d, value);
        }
    }
}

/*!
 * Broadcasts the value of the worker with index 0 to all the others. This is a
 * binomial tree broadcast method.
 *
 * \param net The current worker onto which to apply the operation
 *
 * \param value The value to be broadcast / receive into.
 */
template <typename T>
static inline
void Broadcast(Group& net, T& value) {
    return BroadcastBinomialTree(net, value);
}

/******************************************************************************/
// Reduce Algorithms

/*!
 * \brief Perform a reduce to the worker with index 0.
 *
 * \details This function aggregates the values of all workers according to a
 * summation operator and sends the aggregate to the root, which is the worker
 * with index 0.
 *
 * \param net The current worker onto which to apply the operation
 *
 * \param value The value to be added to the aggregation
 *
 * \param sum_op A custom summation operator
 */
template <typename T, typename BinarySumOp = std::plus<T> >
static inline
void ReduceToRoot(Group& net, T& value, BinarySumOp sum_op = BinarySumOp()) {
    bool active = true;
    for (size_t d = 1; d < net.num_hosts(); d <<= 1) {
        if (active) {
            if (net.my_host_rank() & d) {
                net.SendTo(net.my_host_rank() - d, value);
                active = false;
            }
            else if (net.my_host_rank() + d < net.num_hosts()) {
                T recv_data;
                net.ReceiveFrom(net.my_host_rank() + d, &recv_data);
                value = sum_op(value, recv_data);
            }
        }
    }
}

/******************************************************************************/
// AllReduce Algorithms

//! \brief   Perform an All-Reduce on the workers.
//! \details This is done by aggregating all values according to a summation
//!          operator and sending them backto all workers.
//!
//! \param   net The current worker onto which to apply the operation
//! \param   value The value to be added to the aggregation
//! \param   sum_op A custom summation operator
template <typename T, typename BinarySumOp = std::plus<T> >
static inline
void AllReduce(Group& net, T& value, BinarySumOp sum_op = BinarySumOp()) {
    ReduceToRoot(net, value, sum_op);
    Broadcast(net, value);
}

//! \brief   Perform an All-Reduce for powers of two. This is done with the
//!          Hypercube algorithm from the ParAlg script.
//!
//! \param   net The current worker onto which to apply the operation
//! \param   value The value to be added to the aggregation
//! \param   sum_op A custom summation operator
template <typename T, typename BinarySumOp = std::plus<T> >
static inline
void AllReduceHypercube(Group& net, T& value, BinarySumOp sum_op = BinarySumOp()) {
    // For each dimension of the hypercube, exchange data between workers with
    // different bits at position d

    static const bool debug = false;

    for (size_t d = 1; d < net.num_hosts(); d <<= 1) {
        // communication peer for this round (hypercube dimension)
        size_t peer = net.my_host_rank() ^ d;

        // Send value to worker with id id ^ d
        if (peer < net.num_hosts()) {
            net.connection(peer).Send(value);
            sLOG << "ALL_REDUCE_HYPERCUBE: Host" << net.my_host_rank()
                 << ": Sending" << value << "to worker" << peer;
        }

        // Receive value from worker with id id ^ d
        T recv_data;
        if (peer < net.num_hosts()) {
            net.connection(peer).Receive(&recv_data);

            // The order of addition is important. The total sum of the smaller
            // hypercube always comes first.
            if (net.my_host_rank() & d)
                value = sum_op(recv_data, value);
            else
                value = sum_op(value, recv_data);

            sLOG << "ALL_REDUCE_HYPERCUBE: Host " << net.my_host_rank()
                 << ": Received " << recv_data
                 << " from worker " << peer << " value = " << value;
        }
    }

    sLOG << "ALL_REDUCE_HYPERCUBE: value after all reduce " << value;
}

//! \}

} // namespace collective

/******************************************************************************/
// Definitions for Forwarders from net::Group

template <typename T, typename BinarySumOp>
void Group::PrefixSum(T& value, BinarySumOp sum_op, bool inclusive) {
    return collective::PrefixSum(*this, value, sum_op, inclusive);
}

//! Calculate exclusive prefix sum
template <typename T, typename BinarySumOp>
void Group::ExPrefixSum(T& value, BinarySumOp sum_op) {
    return collective::PrefixSum(*this, value, sum_op, false);
}

//! Broadcast a value from the worker 0
template <typename T>
void Group::Broadcast(T& value) {
    return collective::Broadcast(*this, value);
}

//! Reduce a value from all workers to the worker 0
template <typename T, typename BinarySumOp>
void Group::ReduceToRoot(T& value, BinarySumOp sum_op) {
    return collective::ReduceToRoot(*this, value, sum_op);
}

//! Reduce a value from all workers to all workers
template <typename T, typename BinarySumOp>
void Group::AllReduce(T& value, BinarySumOp sum_op) {
    return collective::AllReduce(*this, value, sum_op);
}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_COLLECTIVE_HEADER

/******************************************************************************/
