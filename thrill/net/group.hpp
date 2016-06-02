/*******************************************************************************
 * thrill/net/group.hpp
 *
 * net::Group is a collection of net::Connections providing simple MPI-like
 * collectives and point-to-point communication.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Robert Hangu <robert.hangu@gmail.com>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_GROUP_HEADER
#define THRILL_NET_GROUP_HEADER

#include <thrill/common/functional.hpp>
#include <thrill/common/math.hpp>
#include <thrill/net/connection.hpp>

#include <algorithm>
#include <cstring>
#include <functional>
#include <thread>
#include <vector>

namespace thrill {
namespace net {

//! \addtogroup net_layer
//! \{

/*!
 * A network Group is a collection of enumerated communication links, which
 * provides point-to-point communication and MPI-like collective primitives.
 *
 * Each communication link in the Group has a specific rank and a representative
 * class Connection can be accessed via connection().
 *
 * The Group class is abstract, and subclasses must exist for every network
 * implementation.
 */
class Group
{
public:
    //! initializing constructor
    explicit Group(size_t my_rank) : my_rank_(my_rank) { }

    //! non-copyable: delete copy-constructor
    Group(const Group&) = delete;
    //! non-copyable: delete assignment operator
    Group& operator = (const Group&) = delete;
    //! move-constructor: default
    Group(Group&&) = default;
    //! move-assignment operator: default
    Group& operator = (Group&&) = default;

    //! virtual destructor
    virtual ~Group() { }

    //! \name Base Functions
    //! \{

    //! Return our rank among hosts in this group.
    size_t my_host_rank() const { return my_rank_; }

    //! Return number of connections in this group (= number computing hosts)
    virtual size_t num_hosts() const = 0;

    //! Return Connection to client id.
    virtual Connection& connection(size_t id) = 0;

    //! Close
    virtual void Close() = 0;

    //! Construct a network dispatcher object for this group, matching its
    //! internal implementation.
    virtual std::unique_ptr<class Dispatcher> ConstructDispatcher(
        mem::Manager& mem_manager) const = 0;

    //! Number of of 1-factor iterations
    size_t OneFactorSize() const {
        return common::CalcOneFactorSize(num_hosts());
    }

    //! Calculate the peer of this host in the k-th iteration (of 0..p-1) of a
    //! 1-factor based network exchange algorithm.
    size_t OneFactorPeer(size_t round) const {
        return common::CalcOneFactorPeer(round, my_host_rank(), num_hosts());
    }

    //! \}

    //! \name Convenience Functions
    //! \{

    /*!
     * Sends a serializable type to the given peer.
     *
     * \param dest The peer to send the data to.
     * \param data The data to send.
     */
    template <typename T>
    void SendTo(size_t dest, const T& data) {
        connection(dest).Send(data);
    }

    /*!
     * Receives a serializable type from the given peer.
     *
     * \param src The peer to receive the fixed length type from.
     * \param data A pointer to the location where the received data should be stored.
     */
    template <typename T>
    void ReceiveFrom(size_t src, T* data) {
        connection(src).Receive(data);
    }

    //! \}

    //! \name Synchronous Collective Communication Functions
    //! \{

    //! Calculate inclusive prefix sum
    template <typename T, typename BinarySumOp = std::plus<T> >
    void PrefixSum(T& value, BinarySumOp sum_op = BinarySumOp());

    //! Calculate exclusive prefix sum
    template <typename T, typename BinarySumOp = std::plus<T> >
    void ExPrefixSum(T& value, BinarySumOp sum_op = BinarySumOp());

    //! Broadcast a value from the worker "origin"
    template <typename T>
    void Broadcast(T& value, size_t origin = 0);

    //! Reduce a value from all workers to the worker 0
    template <typename T, typename BinarySumOp = std::plus<T> >
    void Reduce(T& value, size_t root = 0, BinarySumOp sum_op = BinarySumOp());

    //! Reduce a value from all workers to all workers
    template <typename T, typename BinarySumOp = std::plus<T> >
    void AllReduce(T& value, BinarySumOp sum_op = BinarySumOp());

    //! \}

    //! \name Additional Synchronous Collective Communication Functions
    //! \{

    template <typename T, typename BinarySumOp = std::plus<T> >
    void PrefixSumDoubling(T& value, BinarySumOp sum_op = BinarySumOp(),
                           bool inclusive = true);

    template <typename T, typename BinarySumOp = std::plus<T> >
    void PrefixSumHypercube(T& value, BinarySumOp sum_op = BinarySumOp());

    template <typename T>
    void BroadcastTrivial(T& value, size_t origin = 0);

    template <typename T>
    void BroadcastBinomialTree(T& value, size_t origin = 0);

    template <typename T, typename BinarySumOp = std::plus<T> >
    void AllReduceSimple(T& value, BinarySumOp sum_op = BinarySumOp());

    template <typename T, typename BinarySumOp = std::plus<T> >
    void AllReduceAtRoot(T& value, BinarySumOp sum_op = BinarySumOp());

    template <typename T, typename BinarySumOp = std::plus<T> >
    void AllReduceHypercube(T& value, BinarySumOp sum_op = BinarySumOp());

    //! \}

protected:
    //! our rank in the network group
    size_t my_rank_;

    //! \name Virtual Synchronous Collectives to Override Implementations
    //! \{

    virtual void PrefixSumPlusUInt32(uint32_t& value);
    virtual void PrefixSumPlusUInt64(uint64_t& value);

    virtual void BroadcastUInt32(uint32_t& value, size_t origin);
    virtual void BroadcastUInt64(uint64_t& value, size_t origin);

    //! \}
};

//! unique pointer to a Group.
using GroupPtr = std::unique_ptr<Group>;

//! Construct a mock Group using a complete graph of local stream sockets for
//! testing, and starts a thread for each client, which gets passed the Group
//! object. This is ideal for testing network communication protocols.
template <typename Group, typename GroupCalled>
void ExecuteGroupThreads(
    const std::vector<std::unique_ptr<Group> >& groups,
    const std::function<void(GroupCalled*)>& thread_function) {
    size_t num_hosts = groups.size();

    // create a thread for each Group object and run user program.
    std::vector<std::thread> threads(num_hosts);

    for (size_t i = 0; i < num_hosts; ++i) {
        threads[i] = std::thread(
            [thread_function, g = groups[i].get()]() {
                return thread_function(g);
            });
    }

    for (size_t i = 0; i < num_hosts; ++i) {
        threads[i].join();
    }

    // tear down mesh by closing all group objects
    for (size_t i = 0; i < num_hosts; ++i) {
        groups[i]->Close();
    }
}

//! Construct a mock or tcp-loopback Group network and run a thread for each
//! client. The selected network implementation is platform dependent and must
//! run without further configuration.
void RunLoopbackGroupTest(
    size_t num_hosts,
    const std::function<void(Group*)>& thread_function);

/******************************************************************************/
// Prefixsum Algorithms

/*!
 * Calculate for every worker his prefix sum.
 *
 * The prefix sum is the aggregation of the values of all workers with lesser
 * index, including himself, according to a summation operator. The run-time is
 * in O(log n).
 *
 * \param net The current group onto which to apply the operation
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
 * \param net The current group onto which to apply the operation
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

template <typename T, typename BinarySumOp>
void Group::PrefixSum(T& value, BinarySumOp sum_op) {
    return PrefixSumDoubling(value, sum_op, true);
}

//! specialization template for plus-prefixsum of uint32_t values.
template <>
inline void Group::PrefixSum(uint32_t& value, std::plus<uint32_t>) {
    return PrefixSumPlusUInt32(value);
}

//! specialization template for plus-prefixsum of uint64_t values.
template <>
inline void Group::PrefixSum(uint64_t& value, std::plus<uint64_t>) {
    return PrefixSumPlusUInt64(value);
}

template <typename T, typename BinarySumOp>
void Group::ExPrefixSum(T& value, BinarySumOp sum_op) {
    return PrefixSumDoubling(value, sum_op, false);
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
 * \param net The current group onto which to apply the operation
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

/*!
 * Broadcasts the value of the worker with index 0 to all the others. This is a
 * binomial tree broadcast method.
 *
 * \param net The current group onto which to apply the operation
 *
 * \param value The value to be broadcast / receive into.
 *
 * \param origin The PE to broadcast value from.
 */
template <typename T>
void Group::Broadcast(T& value, size_t origin) {
    return BroadcastBinomialTree(value, origin);
}

//! specialization template for broadcast of uint32_t values.
template <>
inline void Group::Broadcast(uint32_t& value, size_t origin) {
    return BroadcastUInt32(value, origin);
}

//! specialization template for broadcast of uint64_t values.
template <>
inline void Group::Broadcast(uint64_t& value, size_t origin) {
    return BroadcastUInt64(value, origin);
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
 * \param net The current group onto which to apply the operation
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
 * \param   net The current group onto which to apply the operation
 * \param   value The value to be added to the aggregation
 * \param   sum_op A custom summation operator
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
 * \param net The current peer onto which to apply the operation
 *
 * \param value The value to be broadcast / receive into.
 *
 * \param origin The PE to broadcast value from.
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
 * \param   net The current group onto which to apply the operation
 * \param   value The value to be added to the aggregation
 * \param   sum_op A custom summation operator
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
            connection(peer).SendReceive(value, &recv_data);

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

/*!
 * Perform an All-Reduce on the workers.  This is done by aggregating all values
 * according to a summation operator and sending them backto all workers.
 *
 * \param   net The current group onto which to apply the operation
 * \param   value The value to be added to the aggregation
 * \param   sum_op A custom summation operator
 */
template <typename T, typename BinarySumOp>
void Group::AllReduce(T& value, BinarySumOp sum_op) {
    if (common::IsPowerOfTwo(num_hosts()))
        AllReduceHypercube(value, sum_op);
    else
        AllReduceAtRoot(value, sum_op);
}

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_GROUP_HEADER

/******************************************************************************/
