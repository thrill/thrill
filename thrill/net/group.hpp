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

    /*!
     * Sends and Receives a serializable type from the given peer and returns the value after reduction
     *
     * \param src The peer to receive the fixed length type from.
     * \param data A pointer to the location where the received data should be stored.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T sendReceiveReduce(size_t peer, T value, BinarySumOp sum_op){
        T recv_data;
        connection(peer).SendReceive(value, &recv_data);
        if (my_host_rank() > peer)
            return sum_op(recv_data, value);
        else
            return sum_op(value, recv_data);
    }

    /*!
     * Receives a serializable type from the given peer and returns the value after reduction
     *
     * \param src The peer to receive the fixed length type from.
     * \param data A pointer to the location where the received data should be stored.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T receiveReduce(size_t peer, T value, BinarySumOp sum_op){
        T recv_data;
        connection(peer).Receive(&recv_data);
        if (my_host_rank() > peer)
            return sum_op(recv_data, value);
        else
            return sum_op(value, recv_data);
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

    template <typename T, typename BinarySumOp = std::plus<T> >
    void AllReduceElimination(T& value, BinarySumOp sum_op = BinarySumOp());

    template <typename T, typename BinarySumOp = std::plus<T> >
    void eliminationProcessHost(size_t hostId, size_t groupsSize, size_t remainingHostsCount, size_t sendTo, T *value, BinarySumOp sum_op = BinarySumOp());

    //! \}

protected:
    //! our rank in the network group
    size_t my_rank_;

    //! \name Virtual Synchronous Collectives to Override Implementations
    //! \{

/*[[[perl
  for my $e (
    ["int", "Int"], ["unsigned int", "UnsignedInt"],
    ["long", "Long"], ["unsigned long", "UnsignedLong"],
    ["long long", "LongLong"], ["unsigned long long", "UnsignedLongLong"])
  {
    print "virtual void PrefixSumPlus$$e[1]($$e[0]& value);\n";
    print "virtual void ExPrefixSumPlus$$e[1]($$e[0]& value);\n";
    print "virtual void Broadcast$$e[1]($$e[0]& value, size_t origin);\n";
    print "virtual void AllReducePlus$$e[1]($$e[0]& value);\n";
    print "virtual void AllReduceMinimum$$e[1]($$e[0]& value);\n";
    print "virtual void AllReduceMaximum$$e[1]($$e[0]& value);\n";
  }
]]]*/
    virtual void PrefixSumPlusInt(int& value);
    virtual void ExPrefixSumPlusInt(int& value);
    virtual void BroadcastInt(int& value, size_t origin);
    virtual void AllReducePlusInt(int& value);
    virtual void AllReduceMinimumInt(int& value);
    virtual void AllReduceMaximumInt(int& value);
    virtual void PrefixSumPlusUnsignedInt(unsigned int& value);
    virtual void ExPrefixSumPlusUnsignedInt(unsigned int& value);
    virtual void BroadcastUnsignedInt(unsigned int& value, size_t origin);
    virtual void AllReducePlusUnsignedInt(unsigned int& value);
    virtual void AllReduceMinimumUnsignedInt(unsigned int& value);
    virtual void AllReduceMaximumUnsignedInt(unsigned int& value);
    virtual void PrefixSumPlusLong(long& value);
    virtual void ExPrefixSumPlusLong(long& value);
    virtual void BroadcastLong(long& value, size_t origin);
    virtual void AllReducePlusLong(long& value);
    virtual void AllReduceMinimumLong(long& value);
    virtual void AllReduceMaximumLong(long& value);
    virtual void PrefixSumPlusUnsignedLong(unsigned long& value);
    virtual void ExPrefixSumPlusUnsignedLong(unsigned long& value);
    virtual void BroadcastUnsignedLong(unsigned long& value, size_t origin);
    virtual void AllReducePlusUnsignedLong(unsigned long& value);
    virtual void AllReduceMinimumUnsignedLong(unsigned long& value);
    virtual void AllReduceMaximumUnsignedLong(unsigned long& value);
    virtual void PrefixSumPlusLongLong(long long& value);
    virtual void ExPrefixSumPlusLongLong(long long& value);
    virtual void BroadcastLongLong(long long& value, size_t origin);
    virtual void AllReducePlusLongLong(long long& value);
    virtual void AllReduceMinimumLongLong(long long& value);
    virtual void AllReduceMaximumLongLong(long long& value);
    virtual void PrefixSumPlusUnsignedLongLong(unsigned long long& value);
    virtual void ExPrefixSumPlusUnsignedLongLong(unsigned long long& value);
    virtual void BroadcastUnsignedLongLong(unsigned long long& value, size_t origin);
    virtual void AllReducePlusUnsignedLongLong(unsigned long long& value);
    virtual void AllReduceMinimumUnsignedLongLong(unsigned long long& value);
    virtual void AllReduceMaximumUnsignedLongLong(unsigned long long& value);
// [[[end]]]

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

/*[[[perl
  for my $e (
    ["int", "Int"], ["unsigned int", "UnsignedInt"],
    ["long", "Long"], ["unsigned long", "UnsignedLong"],
    ["long long", "LongLong"], ["unsigned long long", "UnsignedLongLong"])
  {
    print "template <>\n";
    print "inline void Group::PrefixSum($$e[0]& value, std::plus<$$e[0]>) {\n";
    print "    return PrefixSumPlus$$e[1](value);\n";
    print "}\n";

    print "template <>\n";
    print "inline void Group::ExPrefixSum($$e[0]& value, std::plus<$$e[0]>) {\n";
    print "    return ExPrefixSumPlus$$e[1](value);\n";
    print "}\n";

    print "template <>\n";
    print "inline void Group::Broadcast($$e[0]& value, size_t origin) {\n";
    print "    return Broadcast$$e[1](value, origin);\n";
    print "}\n";

    print "template <>\n";
    print "inline void Group::AllReduce($$e[0]& value, std::plus<$$e[0]>) {\n";
    print "    return AllReducePlus$$e[1](value);\n";
    print "}\n";

    print "template <>\n";
    print "inline void Group::AllReduce($$e[0]& value, common::minimum<$$e[0]>) {\n";
    print "    return AllReduceMinimum$$e[1](value);\n";
    print "}\n";

    print "template <>\n";
    print "inline void Group::AllReduce($$e[0]& value, common::maximum<$$e[0]>) {\n";
    print "    return AllReduceMaximum$$e[1](value);\n";
    print "}\n";
  }
]]]*/
template <>
inline void Group::PrefixSum(int& value, std::plus<int>) {
    return PrefixSumPlusInt(value);
}
template <>
inline void Group::ExPrefixSum(int& value, std::plus<int>) {
    return ExPrefixSumPlusInt(value);
}
template <>
inline void Group::Broadcast(int& value, size_t origin) {
    return BroadcastInt(value, origin);
}
template <>
inline void Group::AllReduce(int& value, std::plus<int>) {
    return AllReducePlusInt(value);
}
template <>
inline void Group::AllReduce(int& value, common::minimum<int>) {
    return AllReduceMinimumInt(value);
}
template <>
inline void Group::AllReduce(int& value, common::maximum<int>) {
    return AllReduceMaximumInt(value);
}
template <>
inline void Group::PrefixSum(unsigned int& value, std::plus<unsigned int>) {
    return PrefixSumPlusUnsignedInt(value);
}
template <>
inline void Group::ExPrefixSum(unsigned int& value, std::plus<unsigned int>) {
    return ExPrefixSumPlusUnsignedInt(value);
}
template <>
inline void Group::Broadcast(unsigned int& value, size_t origin) {
    return BroadcastUnsignedInt(value, origin);
}
template <>
inline void Group::AllReduce(unsigned int& value, std::plus<unsigned int>) {
    return AllReducePlusUnsignedInt(value);
}
template <>
inline void Group::AllReduce(unsigned int& value, common::minimum<unsigned int>) {
    return AllReduceMinimumUnsignedInt(value);
}
template <>
inline void Group::AllReduce(unsigned int& value, common::maximum<unsigned int>) {
    return AllReduceMaximumUnsignedInt(value);
}
template <>
inline void Group::PrefixSum(long& value, std::plus<long>) {
    return PrefixSumPlusLong(value);
}
template <>
inline void Group::ExPrefixSum(long& value, std::plus<long>) {
    return ExPrefixSumPlusLong(value);
}
template <>
inline void Group::Broadcast(long& value, size_t origin) {
    return BroadcastLong(value, origin);
}
template <>
inline void Group::AllReduce(long& value, std::plus<long>) {
    return AllReducePlusLong(value);
}
template <>
inline void Group::AllReduce(long& value, common::minimum<long>) {
    return AllReduceMinimumLong(value);
}
template <>
inline void Group::AllReduce(long& value, common::maximum<long>) {
    return AllReduceMaximumLong(value);
}
template <>
inline void Group::PrefixSum(unsigned long& value, std::plus<unsigned long>) {
    return PrefixSumPlusUnsignedLong(value);
}
template <>
inline void Group::ExPrefixSum(unsigned long& value, std::plus<unsigned long>) {
    return ExPrefixSumPlusUnsignedLong(value);
}
template <>
inline void Group::Broadcast(unsigned long& value, size_t origin) {
    return BroadcastUnsignedLong(value, origin);
}
template <>
inline void Group::AllReduce(unsigned long& value, std::plus<unsigned long>) {
    return AllReducePlusUnsignedLong(value);
}
template <>
inline void Group::AllReduce(unsigned long& value, common::minimum<unsigned long>) {
    return AllReduceMinimumUnsignedLong(value);
}
template <>
inline void Group::AllReduce(unsigned long& value, common::maximum<unsigned long>) {
    return AllReduceMaximumUnsignedLong(value);
}
template <>
inline void Group::PrefixSum(long long& value, std::plus<long long>) {
    return PrefixSumPlusLongLong(value);
}
template <>
inline void Group::ExPrefixSum(long long& value, std::plus<long long>) {
    return ExPrefixSumPlusLongLong(value);
}
template <>
inline void Group::Broadcast(long long& value, size_t origin) {
    return BroadcastLongLong(value, origin);
}
template <>
inline void Group::AllReduce(long long& value, std::plus<long long>) {
    return AllReducePlusLongLong(value);
}
template <>
inline void Group::AllReduce(long long& value, common::minimum<long long>) {
    return AllReduceMinimumLongLong(value);
}
template <>
inline void Group::AllReduce(long long& value, common::maximum<long long>) {
    return AllReduceMaximumLongLong(value);
}
template <>
inline void Group::PrefixSum(unsigned long long& value, std::plus<unsigned long long>) {
    return PrefixSumPlusUnsignedLongLong(value);
}
template <>
inline void Group::ExPrefixSum(unsigned long long& value, std::plus<unsigned long long>) {
    return ExPrefixSumPlusUnsignedLongLong(value);
}
template <>
inline void Group::Broadcast(unsigned long long& value, size_t origin) {
    return BroadcastUnsignedLongLong(value, origin);
}
template <>
inline void Group::AllReduce(unsigned long long& value, std::plus<unsigned long long>) {
    return AllReducePlusUnsignedLongLong(value);
}
template <>
inline void Group::AllReduce(unsigned long long& value, common::minimum<unsigned long long>) {
    return AllReduceMinimumUnsignedLongLong(value);
}
template <>
inline void Group::AllReduce(unsigned long long& value, common::maximum<unsigned long long>) {
    return AllReduceMaximumUnsignedLongLong(value);
}
// [[[end]]]

//! \}

} // namespace net
} // namespace thrill

#include <thrill/net/collective.hpp>

#endif // !THRILL_NET_GROUP_HEADER

/******************************************************************************/
