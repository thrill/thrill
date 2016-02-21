/*******************************************************************************
 * thrill/net/group.hpp
 *
 * net::Group is a collection of net::Connections providing simple MPI-like
 * collectives and point-to-point communication.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_GROUP_HEADER
#define THRILL_NET_GROUP_HEADER

#include <thrill/common/math.hpp>
#include <thrill/net/connection.hpp>

#include <algorithm>
#include <cstring>
#include <functional>
#include <thread>
#include <vector>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
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
    virtual Connection & connection(size_t id) = 0;

    //! Close
    virtual void Close() = 0;

    //! Construct a network dispatcher object for this group, matching its
    //! internal implementation.
    virtual mem::unique_ptr<class Dispatcher> ConstructDispatcher(
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

    /**
     * Sends a serializable type to the given peer.
     *
     * \param dest The peer to send the data to.
     * \param data The data to send.
     */
    template <typename T>
    void SendTo(size_t dest, const T& data) {
        connection(dest).Send(data);
    }

    /**
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
    void PrefixSum(T& value, BinarySumOp sum_op = BinarySumOp(),
                   bool inclusive = true);

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

protected:
    //! our rank in the network group
    size_t my_rank_;
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
            std::bind(thread_function, groups[i].get()));
    }

    for (size_t i = 0; i < num_hosts; ++i) {
        threads[i].join();
    }

    // tear down mesh by closing all group objects
    for (size_t i = 0; i < num_hosts; ++i) {
        groups[i]->Close();
    }
}

//! Construct a mock or tcp-lookback Group network and run a thread for each
//! client. The selected network implementation is platform dependent and must
//! run without further configuration.
void RunLoopbackGroupTest(
    size_t num_hosts,
    const std::function<void(Group*)>& thread_function);

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_GROUP_HEADER

/******************************************************************************/
