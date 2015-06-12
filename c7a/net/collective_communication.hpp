/*******************************************************************************
 * c7a/net/collective_communication.hpp
 *
 * This file provides collective communication primitives, which are to be used
 * with c7a::net::Groups.
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_COLLECTIVE_COMMUNICATION_HEADER
#define C7A_NET_COLLECTIVE_COMMUNICATION_HEADER

#include <c7a/net/group.hpp>
#include <mutex>
#include <condition_variable>

namespace c7a {
namespace net {

//! \name Collective Operations
//! \{

//! @brief   Calculate for every worker his prefix sum.
//! @details The prefix sum is the aggregatation of the values of all workers
//!          with lesser index, including himself, according to a summation
//!          operator. This function currently only supports worker numbers
//!          which are powers of two.
//!
//! @param   net The current worker onto which to apply the operation
//! @param   value The value to be summed up
//! @param   sumOp A custom summation operator
template <typename T, typename BinarySumOp = common::SumOp<T> >
static void PrefixSum(Group& net, T& value, BinarySumOp sumOp = BinarySumOp()) {
    T total_sum = value;

    static const bool debug = false;

    for (size_t d = 1; d < net.Size(); d <<= 1)
    {
        // Send total sum of this hypercube to worker with id = id XOR d
        if ((net.MyRank() ^ d) < net.Size()) {
            net.connection(net.MyRank() ^ d).Send(total_sum);
            sLOG << "PREFIX_SUM: Worker" << net.MyRank() << ": Sending" << total_sum
                 << "to worker" << (net.MyRank() ^ d);
        }

        // Receive total sum of smaller hypercube from worker with id = id XOR d
        T recv_data;
        if ((net.MyRank() ^ d) < net.Size()) {
            net.connection(net.MyRank() ^ d).Receive(&recv_data);
            total_sum = sumOp(total_sum, recv_data);
            // Variable 'value' represents the prefix sum of this worker
            if (net.MyRank() & d)
                value = sumOp(value, recv_data);
            sLOG << "PREFIX_SUM: Worker" << net.MyRank() << ": Received" << recv_data
                 << "from worker" << (net.MyRank() ^ d)
                 << "value =" << value;
        }
    }

    sLOG << "PREFIX_SUM: Worker" << net.MyRank()
         << ": value after prefix sum =" << value;
}

//! @brief   Perform a reduce to the worker with index 0.
//! @details This function aggregates the values of all workers according to a
//!          summation operator and sends the aggregate to the root, which is
//!          the worker with index 0.
//!
//! @param   net The current worker onto which to apply the operation
//! @param   value The value to be added to the aggregation
//! @param   sumOp A custom summation operator
template <typename T, typename BinarySumOp = common::SumOp<T> >
void ReduceToRoot(Group& net, T& value, BinarySumOp sumOp = BinarySumOp()) {
    bool active = true;
    for (size_t d = 1; d < net.Size(); d <<= 1) {
        if (active) {
            if (net.MyRank() & d) {
                net.connection(net.MyRank() - d).Send(value);
                active = false;
            }
            else if (net.MyRank() + d < net.Size()) {
                T recv_data;
                net.connection(net.MyRank() + d).Receive(&recv_data);
                value = sumOp(value, recv_data);
            }
        }
    }
}

//! @brief   Broadcasts the value of the worker with index 0 to all the others.
//!
//! @param   net The current worker onto which to apply the operation
//! @param   value The value to be added to the aggregation
//! @param   sumOp A custom summation operator
template <typename T>
void Broadcast(Group& net, T& value) {
    if (net.MyRank() > 0) {
        ClientId from;
        net.ReceiveFromAny(&from, &value);
    }
    for (size_t d = 1, i = 0; ((net.MyRank() >> i) & 1) == 0 && d < net.Size(); d <<= 1, ++i) {
        if (net.MyRank() + d < net.Size()) {
            net.connection(net.MyRank() + d).Send(value);
        }
    }
}

//! @brief   Perform an All-Reduce on the workers. 
//! @details This is done by aggregating all values according to a summation
//!          operator and sending them backto all workers.
//!
//! @param   net The current worker onto which to apply the operation
//! @param   value The value to be added to the aggregation
//! @param   sumOp A custom summation operator
template <typename T, typename BinarySumOp = common::SumOp<T> >
void AllReduce(Group& net, T& value, BinarySumOp sumOp = BinarySumOp()) {
    ReduceToRoot(net, value, sumOp);
    Broadcast(net, value);
}

//! @brief   Perform a barrier for all workers.
//! @details All workers synchronize to this point. This operation can be used if
//!          one wants to be sure that all workers continue their operation
//!          synchronously after this point.
//!
//! @param   mtx A common mutex onto which to lock
//! @param   cv  A condition variable which locks on the given mutex
//! @param   num_workers The total number of workers in the network
static inline
void ThreadBarrier(std::mutex &mtx, std::condition_variable &cv, int &num_workers) {
    std::unique_lock<std::mutex> lck(mtx);
    if (num_workers > 1) {
        --num_workers;
        while (num_workers > 0) cv.wait(lck);
    }
    else {
        --num_workers;
        cv.notify_all();
    }
}

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_COLLECTIVE_COMMUNICATION_HEADER

/******************************************************************************/
