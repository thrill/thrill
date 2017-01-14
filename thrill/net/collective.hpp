/*******************************************************************************
 * thrill/net/collective.hpp
 *
 * net::Group is a collection of net::Connections providing simple MPI-like
 * collectives and point-to-point communication.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Robert Hangu <robert.hangu@gmail.com>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Lorenz Hübschle-Schneider <lorenz@4z2.de>
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

//! \addtogroup net_layer
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
 * \param value The value to be summed up
 * \param sum_op A custom summation operator
 * \param inclusive Inclusive prefix sum if true (default)
 */
template <typename T, typename BinarySumOp>
void Group::PrefixSumDoubling(T& value, BinarySumOp sum_op, bool inclusive) {
    static constexpr bool debug = false;

    bool first = true;
    // Use a copy, in case of exclusive, we have to forward
    // something that's not our result.
    T to_forward = value;

    // This is based on the pointer-doubling algorithm presented in the ParAlg
    // script, which is used for list ranking.
    for (size_t d = 1; d < num_hosts(); d <<= 1) {

        if (my_host_rank() + d < num_hosts()) {
            sLOG << "Host" << my_host_rank()
                 << ": sending to" << my_host_rank() + d;
            SendTo(my_host_rank() + d, to_forward);
        }

        if (my_host_rank() >= d) {
            T recv_value;
            ReceiveFrom(my_host_rank() - d, &recv_value);
            sLOG << "Host" << my_host_rank()
                 << ": receiving from" << my_host_rank() - d;

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
    if (!inclusive && my_host_rank() == 0)
        value = T();
}

/*!
 * \brief Calculate for every worker his prefix sum. Works only for worker
 * numbers which are powers of two.
 *
 * \details The prefix sum is an aggregatation of the values of all workers with
 * smaller index, including itself, according to an associative summation
 * operator. This function currently only supports worker numbers which are
 * powers of two.
 *
 * \param value The value to be summed up
 *
 * \param sum_op A custom summation operator
 */
template <typename T, typename BinarySumOp>
void Group::PrefixSumHypercube(T& value, BinarySumOp sum_op) {
    T total_sum = value;

    static constexpr bool debug = false;

    for (size_t d = 1; d < num_hosts(); d <<= 1)
    {
        // communication peer for this round (hypercube dimension)
        size_t peer = my_host_rank() ^ d;

        // Send total sum of this hypercube to worker with id = id XOR d
        if (peer < num_hosts()) {
            SendTo(peer, total_sum);
            sLOG << "PREFIX_SUM: host" << my_host_rank()
                 << ": sending to peer" << peer;
        }

        // Receive total sum of smaller hypercube from worker with id = id XOR d
        T recv_data;
        if (peer < num_hosts()) {
            ReceiveFrom(peer, &recv_data);
            // The order of addition is important. The total sum of the smaller
            // hypercube always comes first.
            if (my_host_rank() & d)
                total_sum = sum_op(recv_data, total_sum);
            else
                total_sum = sum_op(total_sum, recv_data);
            // Variable 'value' represents the prefix sum of this worker
            if (my_host_rank() & d)
                // The order of addition is respected the same way as above.
                value = sum_op(recv_data, value);
            sLOG << "PREFIX_SUM: host" << my_host_rank()
                 << ": received from peer" << peer;
        }
    }

    sLOG << "PREFIX_SUM: host" << my_host_rank() << ": done";
}

//! select prefixsum implementation (often due to total number of processors)
template <typename T, typename BinarySumOp>
void Group::PrefixSumSelect(T& value, BinarySumOp sum_op, bool inclusive) {
    return PrefixSumDoubling(value, sum_op, inclusive);
}

template <typename T, typename BinarySumOp>
void Group::PrefixSum(T& value, BinarySumOp sum_op) {
    return PrefixSumSelect(value, sum_op, true);
}

template <typename T, typename BinarySumOp>
void Group::ExPrefixSum(T& value, BinarySumOp sum_op) {
    return PrefixSumSelect(value, sum_op, false);
}

/******************************************************************************/
// Broadcast Algorithms

/*!
 * Broadcasts the value of the peer with index 0 to all the others. This is a
 * trivial broadcast from peer 0.
 *
 * \param value The value to be broadcast / receive into.
 *
 * \param origin The PE to broadcast value from.
 */
template <typename T>
void Group::BroadcastTrivial(T& value, size_t origin) {

    if (my_host_rank() == origin) {
        // send value to all peers
        for (size_t p = 0; p < num_hosts(); ++p) {
            if (p != origin)
                SendTo(p, value);
        }
    }
    else {
        // receive from origin
        ReceiveFrom(origin, &value);
    }
}

/*!
 * Broadcasts the value of the worker with index "origin" to all the
 * others. This is a binomial tree broadcast method.
 *
 * \param value The value to be broadcast / receive into.
 *
 * \param origin The PE to broadcast value from.
 */
template <typename T>
void Group::BroadcastBinomialTree(T& value, size_t origin) {
    static constexpr bool debug = false;

    size_t num_hosts = this->num_hosts();
    // calculate rank in cyclically shifted binomial tree
    size_t my_rank = (my_host_rank() + num_hosts - origin) % num_hosts;
    size_t r = 0, d = 1;
    // receive from predecessor
    if (my_rank > 0) {
        // our predecessor is p with the lowest one bit flipped to zero. this
        // also counts the number of rounds we have to send out messages later.
        r = common::ffs(my_rank) - 1;
        d <<= r;
        size_t from = ((my_rank ^ d) + origin) % num_hosts;
        sLOG << "Broadcast: rank" << my_rank << "receiving from" << from
             << "in round" << r;
        ReceiveFrom(from, &value);
    }
    else {
        d = common::RoundUpToPowerOfTwo(num_hosts);
    }
    // send to successors
    for (d >>= 1; d > 0; d >>= 1, ++r) {
        if (my_rank + d < num_hosts) {
            size_t to = (my_rank + d + origin) % num_hosts;
            sLOG << "Broadcast: rank" << my_rank << "round" << r << "sending to"
                 << to;
            SendTo(to, value);
        }
    }
}

//! select broadcast implementation (often due to total number of processors)
template <typename T>
void Group::BroadcastSelect(T& value, size_t origin) {
    return BroadcastBinomialTree(value, origin);
}

/*!
 * Broadcasts the value of the worker with index 0 to all the others. This is a
 * binomial tree broadcast method.
 *
 * \param value The value to be broadcast / receive into.
 *
 * \param origin The PE to broadcast value from.
 */
template <typename T>
void Group::Broadcast(T& value, size_t origin) {
    return BroadcastSelect(value, origin);
}

/******************************************************************************/
// Reduce Algorithms

/*!
 * \brief Perform a reduction on all workers in a group.
 *
 * \details This function aggregates the values of all workers in the group
 * according with a specified reduction operator. The result will be returned in
 * the input variable at the root node.
 *
 * \param value The input value to be used in the reduction. Will be overwritten
 * with the result (on the root) or arbitrary data (on other ranks).
 *
 * \param root The rank of the root
 *
 * \param sum_op A custom reduction operator (optional)
 */
template <typename T, typename BinarySumOp>
void Group::Reduce(T& value, size_t root, BinarySumOp sum_op) {
    static constexpr bool debug = false;
    const size_t num_hosts = this->num_hosts();
    const size_t my_rank = my_host_rank() + num_hosts;
    const size_t shifted_rank = (my_rank - root) % num_hosts;
    sLOG << my_host_rank() << "shifted_rank" << shifted_rank;

    for (size_t d = 1; d < num_hosts; d <<= 1) {
        if (shifted_rank & d) {
            sLOG << "Reduce" << my_host_rank()
                 << "->" << (my_rank - d) % num_hosts << "/"
                 << shifted_rank << "->" << shifted_rank - d;
            SendTo((my_rank - d) % num_hosts, value);
            break;
        }
        else if (shifted_rank + d < num_hosts) {
            sLOG << "Reduce" << my_host_rank()
                 << "<-" << (my_rank + d) % num_hosts << "/"
                 << shifted_rank << "<-" << shifted_rank + d;
            T recv_data;
            ReceiveFrom((my_rank + d) % num_hosts, &recv_data);
            value = sum_op(value, recv_data);
        }
    }
}

/******************************************************************************/
// AllReduce Algorithms

/*!
 * Perform an All-Reduce on the workers. This is done by aggregating all values
 * according to a summation operator and sending them backto all workers.
 *
 * \param value The value to be added to the aggregation
 * \param sum_op A custom summation operator
 */
template <typename T, typename BinarySumOp>
void Group::AllReduceSimple(T& value, BinarySumOp sum_op) {
    Reduce(value, 0, sum_op);
    Broadcast(value, 0);
}

/*!
 * Broadcasts the value of the peer with index 0 to all the others. This is a
 * trivial broadcast from peer 0.
 *
 * \param value The value to be broadcast / receive into.
 *
 * \param sum_op A custom summation operator
 */
template <typename T, typename BinarySumOp>
void Group::AllReduceAtRoot(T& value, BinarySumOp sum_op) {

    if (my_host_rank() == 0) {
        // receive value from all peers
        for (size_t p = 1; p < num_hosts(); ++p) {
            T recv_value;
            ReceiveFrom(p, &recv_value);
            value = sum_op(value, recv_value);
        }
        // send reduced value back to all peers
        for (size_t p = 1; p < num_hosts(); ++p) {
            SendTo(p, value);
        }
    }
    else {
        // send to root host
        SendTo(0, value);
        // receive value back from root
        ReceiveFrom(0, &value);
    }
}

/*!
 * Perform an All-Reduce for powers of two. This is done with the Hypercube
 * algorithm from the ParAlg script.
 *
 * \param value The value to be added to the aggregation
 * \param sum_op A custom summation operator
 */
template <typename T, typename BinarySumOp>
void Group::AllReduceHypercube(T& value, BinarySumOp sum_op) {
    // For each dimension of the hypercube, exchange data between workers with
    // different bits at position d

    // static constexpr bool debug = false;

    for (size_t d = 1; d < num_hosts(); d <<= 1) {
        // communication peer for this round (hypercube dimension)
        size_t peer = my_host_rank() ^ d;

        // SendReceive value to worker with id id ^ d
        if (peer < num_hosts()) {
            // sLOG << "ALL_REDUCE_HYPERCUBE: Host" << my_host_rank()
            //      << ": Sending" << value << "to worker" << peer;

            T recv_data;
            if (my_host_rank() & d) {
                connection(peer).SendReceive(value, &recv_data);
            }
            else {
                connection(peer).ReceiveSend(value, &recv_data);
            }

            // The order of addition is important. The total sum of the smaller
            // hypercube always comes first.
            if (my_host_rank() & d)
                value = sum_op(recv_data, value);
            else
                value = sum_op(value, recv_data);

            // sLOG << "ALL_REDUCE_HYPERCUBE: Host " << my_host_rank()
            //      << ": Received " << recv_data
            //      << " from worker " << peer << " value = " << value;
        }
    }

    // sLOG << "ALL_REDUCE_HYPERCUBE: value after all reduce " << value;
}

//! select allreduce implementation (often due to total number of processors)
template <typename T, typename BinarySumOp>
void Group::AllReduceSelect(T& value, BinarySumOp sum_op) {
    if (common::IsPowerOfTwo(num_hosts()))
        AllReduceHypercube(value, sum_op);
    else
        AllReduceAtRoot(value, sum_op);
}

/*!
 * Perform an All-Reduce on the workers.  This is done by aggregating all values
 * according to a summation operator and sending them backto all workers.
 *
 * \param   value The value to be added to the aggregation
 * \param   sum_op A custom summation operator
 */
template <typename T, typename BinarySumOp>
void Group::AllReduce(T& value, BinarySumOp sum_op) {
    return AllReduceSelect(value, sum_op);
}

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_COLLECTIVE_HEADER

/******************************************************************************/
