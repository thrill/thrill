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
 * Copyright (C) 2017 Nejmeddine Douma <nejmeddine.douma@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_COLLECTIVE_HEADER
#define THRILL_NET_COLLECTIVE_HEADER

#include <thrill/common/functional.hpp>
#include <thrill/net/group.hpp>
#include <tlx/math/ffs.hpp>
#include <tlx/math/is_power_of_two.hpp>
#include <tlx/math/round_to_power_of_two.hpp>
#include <tlx/math/integer_log2.hpp>

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
        r = tlx::ffs(my_rank) - 1;
        d <<= r;
        size_t from = ((my_rank ^ d) + origin) % num_hosts;
        sLOG << "Broadcast: rank" << my_rank << "receiving from" << from
             << "in round" << r;
        ReceiveFrom(from, &value);
    }
    else {
        d = tlx::round_up_to_power_of_two(num_hosts);
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
// AllGather Algorithms

template <typename T>
void Group::AllGatherRecursiveDoublingPowerOfTwo(T* values, size_t n) {
    size_t num_hosts = this->num_hosts();
    size_t my_rank 	 = my_host_rank();
    size_t d = tlx::integer_log2_ceil(num_hosts);

    for (size_t j = 0; j < d; ++j) {
        size_t peer    = my_rank ^ (0x1 << j);
        // index of first element to be sent
        size_t snd_pos = (~((0x1 << j) - 1) & my_rank) * n;
        // index of first element to be received
        size_t rcv_pos = (~((0x1 << j) - 1) & peer) * n;
        // number of elements to be sent/received
        size_t ins_n   = (0x1 << j) * n;

        connection(peer).SendReceive(values + snd_pos, values + rcv_pos, ins_n);
    }
}

template <typename T>
void Group::AllGatherBruck(T* values, size_t n) {
    size_t num_hosts = this->num_hosts();
    size_t my_rank   = my_host_rank();
    size_t size	     = num_hosts * n;
    std::vector<T> temp(size);

    for (size_t i = 0; i < n; ++i) {
        temp[i] = values[i];
    }

    for (size_t j = 0; (0x1U << j) < num_hosts; ++j) {
        size_t snd_peer = (my_rank + num_hosts - (0x1 << j)) % num_hosts;
        size_t rcv_peer = (my_rank + (0x1 << j)) % num_hosts;
        // position for received data
        size_t ins_pos  = (0x1 << j) * n;
        // number of elements to be sent/received
        size_t ins_n    = std::min((0x1 << j) * n, size - ins_pos);

        if ((0x1 << j) & my_rank) {
            connection(rcv_peer).ReceiveN(temp.data() + ins_pos, ins_n);
            connection(snd_peer).SendN(temp.data(), ins_n);
        }
        else {
            connection(snd_peer).SendN(temp.data(), ins_n);
            connection(rcv_peer).ReceiveN(temp.data() + ins_pos, ins_n);
        }
    }

    // local reorder: shift whole array by my_rank*n to the right
    for (size_t i = 0; i < size; ++i) {
        values[i] = temp[(i + size - my_rank*n) % size];
    }

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
 * \note This method is no longer used, but it is kept here for reference
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
            // LOG << "ALL_REDUCE_HYPERCUBE: Host" << my_host_rank()
            //     << ": Sending" << value << " to worker" << peer;

            // The order of addition is important. The total sum of the smaller
            // hypercube always comes first.
            T recv_data;
            if (my_host_rank() & d) {
                connection(peer).SendReceive(&value, &recv_data);
                value = sum_op(recv_data, value);
            }
            else {
                connection(peer).ReceiveSend(value, &recv_data);
                value = sum_op(value, recv_data);
            }

            // LOG << "ALL_REDUCE_HYPERCUBE: Host " << my_host_rank()
            //     << ": Received " << recv_data
            //     << " from worker " << peer << " value = " << value;
        }
    }
}

/*!
 * Perform an All-Reduce using the elimination protocol described in
 * R. Rabenseifner and J. L. Traeff. "More Efficient Reduction Algorithms for
 * Non-Power-of-Two Number of Processors in Message-Passing Parallel Systems."
 * In Recent Advances in Parallel Virtual Machine and Message Passing Interface,
 * 36–46. LNCS 3241. Springer, 2004.
 *
 * \param value The value to be added to the aggregation
 * \param sum_op A custom summation operator
 */
template <typename T, typename BinarySumOp>
void Group::AllReduceElimination(T& value, BinarySumOp sum_op) {
    AllReduceEliminationProcess(
        my_host_rank(), 1, num_hosts(), 0, value, sum_op);
}

template <typename T, typename BinarySumOp>
T Group::SendReceiveReduce(size_t peer, const T& value, BinarySumOp sum_op) {
    T recv_data;
    if (my_host_rank() > peer) {
        connection(peer).SendReceive(&value, &recv_data);
        return sum_op(recv_data, value);
    }
    else {
        connection(peer).ReceiveSend(value, &recv_data);
        return sum_op(value, recv_data);
    }
}

//! used for the recursive implementation of the elimination protocol
template <typename T, typename BinarySumOp>
void Group::AllReduceEliminationProcess(
    size_t host_id, size_t group_size, size_t remaining_hosts,
    size_t send_to, T& value, BinarySumOp sum_op) {

    // static const bool debug = false;

    // send_to == 0 => no eliminated host waiting to receive from current host,
    // host 0 is never eliminated

    size_t group_count = remaining_hosts / group_size;
    if (group_count % 2 == 0) {
        // only hypercube
        size_t peer = host_id ^ group_size;
        if (peer < remaining_hosts) {
            value = SendReceiveReduce(peer, value, sum_op);
        }
    }
    else {
        // check if my rank is in 3-2 elimination zone
        size_t host_group = host_id / group_size;
        if (host_group >= group_count - 3) {
            // take part in the 3-2 elimination
            if (host_group == group_count - 1) {
                size_t peer = (host_id ^ group_size) - 2 * group_size;
                SendTo(peer, value);
                ReceiveFrom(peer, &value);
            }
            else if (host_group == group_count - 2) {
                size_t peer = (host_id ^ group_size) + 2 * group_size;

                T recv_data;
                ReceiveFrom(peer, &recv_data);
                if (my_host_rank() > peer)
                    value = sum_op(recv_data, value);
                else
                    value = sum_op(value, recv_data);

                // important for gathering
                send_to = peer;

                peer = host_id ^ group_size;
                value = SendReceiveReduce(peer, value, sum_op);
            }
            else if (host_group == group_count - 3) {
                size_t peer = host_id ^ group_size;
                value = SendReceiveReduce(peer, value, sum_op);
            }
        }
        else {
            // no elimination, execute hypercube
            size_t peer = host_id ^ group_size;
            if (peer < remaining_hosts) {
                value = SendReceiveReduce(peer, value, sum_op);
            }
        }
        remaining_hosts -= group_size;
    }
    group_size <<= 1;

    // recursion
    if (group_size < remaining_hosts) {
        AllReduceEliminationProcess(
            host_id, group_size, remaining_hosts, send_to,
            value, sum_op);
    }
    else if (send_to != 0) {
        SendTo(send_to, value);
    }
}

//! select allreduce implementation (often due to total number of processors)
template <typename T, typename BinarySumOp>
void Group::AllReduceSelect(T& value, BinarySumOp sum_op) {
    // always use 3-2-elimination reduction method.
    AllReduceElimination(value, sum_op);
    /*if (tlx::is_power_of_two(num_hosts()))
        AllReduceHypercube(value, sum_op);
    else
        AllReduceAtRoot(value, sum_op);*/
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
