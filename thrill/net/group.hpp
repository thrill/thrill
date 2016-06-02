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
    //! Do not use these directly in user code.
    //! \{

    template <typename T, typename BinarySumOp = std::plus<T> >
    void PrefixSumSelect(T& value, BinarySumOp sum_op = BinarySumOp(),
                         bool inclusive = true);

    template <typename T, typename BinarySumOp = std::plus<T> >
    void PrefixSumDoubling(T& value, BinarySumOp sum_op = BinarySumOp(),
                           bool inclusive = true);

    template <typename T, typename BinarySumOp = std::plus<T> >
    void PrefixSumHypercube(T& value, BinarySumOp sum_op = BinarySumOp());

    /**************************************************************************/

    template <typename T>
    void BroadcastSelect(T& value, size_t origin = 0);

    template <typename T>
    void BroadcastTrivial(T& value, size_t origin = 0);

    template <typename T>
    void BroadcastBinomialTree(T& value, size_t origin = 0);

    /**************************************************************************/

    template <typename T, typename BinarySumOp = std::plus<T> >
    void AllReduceSelect(T& value, BinarySumOp sum_op = BinarySumOp());

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

    virtual void ExPrefixSumPlusUInt32(uint32_t& value);
    virtual void ExPrefixSumPlusUInt64(uint64_t& value);

    virtual void BroadcastUInt32(uint32_t& value, size_t origin);
    virtual void BroadcastUInt64(uint64_t& value, size_t origin);

    virtual void AllReducePlusUInt32(uint32_t& value);
    virtual void AllReducePlusUInt64(uint64_t& value);

    virtual void AllReduceMaxUInt32(uint32_t& value);
    virtual void AllReduceMaxUInt64(uint64_t& value);

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
// Template Specializations to call Virtual Overrides

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

//! specialization template for plus-prefixsum of uint32_t values.
template <>
inline void Group::ExPrefixSum(uint32_t& value, std::plus<uint32_t>) {
    return ExPrefixSumPlusUInt32(value);
}

//! specialization template for plus-prefixsum of uint64_t values.
template <>
inline void Group::ExPrefixSum(uint64_t& value, std::plus<uint64_t>) {
    return ExPrefixSumPlusUInt64(value);
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

//! specialization template for plus-allreduce of uint32_t values.
template <>
inline void Group::AllReduce(uint32_t& value, std::plus<uint32_t>) {
    return AllReducePlusUInt32(value);
}

//! specialization template for plus-allreduce of uint64_t values.
template <>
inline void Group::AllReduce(uint64_t& value, std::plus<uint64_t>) {
    return AllReducePlusUInt64(value);
}

//! specialization template for max-allreduce of uint32_t values.
template <>
inline void Group::AllReduce(uint32_t& value, common::maximum<uint32_t>) {
    return AllReduceMaxUInt32(value);
}

//! specialization template for max-allreduce of uint64_t values.
template <>
inline void Group::AllReduce(uint64_t& value, common::maximum<uint64_t>) {
    return AllReduceMaxUInt64(value);
}

//! \}

} // namespace net
} // namespace thrill

#include <thrill/net/collective.hpp>

#endif // !THRILL_NET_GROUP_HEADER

/******************************************************************************/
