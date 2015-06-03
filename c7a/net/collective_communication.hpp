/*******************************************************************************
 * c7a/net/collective_communication.hpp
 *
 * Part of Project c7a.
 *
 * This file provides collective communication primitives, which are to be used
 * with c7a::net::Groups.
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_COLL_COMM_HEADER
#define C7A_NET_COLL_COMM_HEADER

#include <c7a/net/group.hpp>

namespace c7a {
namespace net {

    //! \name Collective Operations
    //! \{
 
    //! Calculate for every worker his prefix sum, which is the sum of the
    //! values of all previous workers including himself.
    template <typename T, typename BinarySumOp = common::SumOp<T> >
    static void PrefixSum(Group &net, T& value, BinarySumOp sumOp = BinarySumOp()) {
        // The total sum in the current hypercube. This is stored, because later,
        // bigger hypercubes need this value.
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

    //! Perform a binomial tree reduce to the worker with index 0
    template <typename T, typename BinarySumOp = common::SumOp<T> >
    void ReduceToRoot(Group &net, T& value, BinarySumOp sumOp = BinarySumOp()) {
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

    //! Binomial-broadcasts the value of the worker with index 0 to all the others
    template <typename T>
    static void Broadcast(Group &net, T& value) {
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

    //! Perform an All-Reduce on the workers by aggregating all values and sending
    //! them backto all workers
    template <typename T, typename BinarySumOp = common::SumOp<T> >
    static void AllReduce(Group &net, T& value, BinarySumOp sumOp = BinarySumOp()) {
        ReduceToRoot(net, value, sumOp);
        Broadcast(net, value);
    }

    //! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_COLL_COMM_HEADER
