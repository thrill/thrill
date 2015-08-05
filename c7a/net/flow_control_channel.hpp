/*******************************************************************************
 * c7a/net/flow_control_channel.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_FLOW_CONTROL_CHANNEL_HEADER
#define C7A_NET_FLOW_CONTROL_CHANNEL_HEADER

#include <c7a/common/cyclic_barrier.hpp>
#include <c7a/common/functional.hpp>
#include <c7a/net/group.hpp>

#include <string>
#include <vector>

namespace c7a {
namespace net {

//! \addtogroup net Network Communication
//! \{

/**
 * @brief Provides a blocking collection for communication.
 * @details This wraps a raw net group and should be used for
 * flow control with integral types.
 *
 * Important notice on threading: It is not allowed to call two different methods of two different
 * instances of FlowControlChannel simultaniously by different threads, since the internal synchronization state
 * (the barrier) is shared globally.
 *
 * The implementations will be replaced by better/decentral versions.
 *
 * This class is probably the worst thing I've ever coded (ej).
 */
class FlowControlChannel
{
protected:
    /**
     * The group associated with this channel.
     */
    net::Group& group;
    /**
     * The local id.
     */
    int id;

    /**
     * The count of all workers connected to this group.
     */
    int count;

    /**
     * The id of the worker thread associated with this flow channel.
     */
    int threadId;

    /**
     * The count of all workers connected to this group.
     */
    int threadCount;

    /**
     * The shared barrier used to synchronize between worker threads on this node.
     */
    common::Barrier& barrier;

    /**
     * A shared memory location to work upon.
     */
    void** shmem;

    /**
     * @brief Sends a value of an integral type T to a certain other worker.
     * @details This method can block if there is unsufficient space
     * in the send buffer. This method may only be called by thread with ID 0.
     *
     * @param destination The id of the worker to send the value to.
     * @param value The value to send.
     */
    template <typename T>
    void SendTo(ClientId destination, T value) {

        assert(threadId == 0); //Only primary thread might send/receive.

        group.connection(destination).Send(value);
    }

    /**
     * @brief Receives a value of an integral type T from a certain other worker.
     * @details This method blocks until the data is received. This method may only be called by thread with ID 0.
     *
     * @param source The id of the worker to receive the value from.
     * @param value A pointer to a memory location where the
     * received value is stored.
     */
    template <typename T>
    void ReceiveFrom(ClientId source, T* value) {

        assert(threadId == 0); //Only primary thread might send/receive.

        group.connection(source).Receive(value);
    }

    template <typename T>
    void SetLocalShared(T* value) {
        assert(*shmem == NULL);
        assert(threadId == 0);
        *shmem = value;
    }

    template <typename T>
    T * GetLocalShared() {
        assert(*shmem != NULL);
        return *(reinterpret_cast<T**>(shmem));
    }

    void ClearLocalShared() {
        assert(threadId == 0);
        *shmem = NULL;
    }

public:
    /**
     * @brief Creates a new instance of this class, wrapping a group.
     */
    explicit FlowControlChannel(net::Group& group, int threadId, int threadCount, common::Barrier& barrier, void** shmem)
        : group(group), id(group.MyRank()), count(group.Size()), threadId(threadId), threadCount(threadCount), barrier(barrier), shmem(shmem) { }

    /**
     * @brief Calculates the prefix sum over all workers, given a certain sum
     * operation.
     * @details This method blocks until the sum is caluclated. Values
     * are applied in order, that means sumOp(sumOp(a, b), c) if a, b, c
     * are the values of workers 0, 2, 3.
     *
     * @param value The local value of this worker.
     * @param sumOp The operation to use for
     * calculating the prefix sum. The default operation is a normal addition.
     * @param inclusive Whether the prefix sum is inclusive or exclusive.
     * @return The prefix sum for the position of this worker.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T PrefixSum(const T& value, BinarySumOp sumOp = BinarySumOp(),
                bool inclusive = true) {

        T res = value;
        //return value when computing non-exclusive prefix sum
        T exclusiveRes = T();
        std::vector<T> localPrefixBuffer(threadCount);

        //Local Reduce
        if (threadId == 0) {
            //Master allocate memory.
            localPrefixBuffer[threadId] = value;
            SetLocalShared(&localPrefixBuffer);
            barrier.Await();
            //Slave store values.
            barrier.Await();

            //Global Prefix

            //Everyone except the first one needs
            //to receive and add.
            if (id != 0) {
                ReceiveFrom(id - 1, &res);
                exclusiveRes = res;
            }

            for (int i = id == 0 ? 1 : 0; i < threadCount; i++) {
                localPrefixBuffer[i] = sumOp(res, localPrefixBuffer[i]);
                res = localPrefixBuffer[i];
            }

            //Everyone except the last one needs to forward.
            if (id != count - 1) {
                SendTo(id + 1, res);
            }

            barrier.Await();
            //Slave get result
            barrier.Await();
            ClearLocalShared();

            res = localPrefixBuffer[threadId];
        }
        else {
            //Master allocate memory.
            barrier.Await();
            //Slave store values.
            (*GetLocalShared<std::vector<T> >())[threadId] = value;
            barrier.Await();
            //Global Prefix
            barrier.Await();
            //Slave get result
            if (inclusive) {
                res = (*GetLocalShared<std::vector<T> >())[threadId];
            }
            else {
                res = (*GetLocalShared<std::vector<T> >())[threadId - 1];
            }
            barrier.Await();
        }
        if (inclusive) {
            return res;
        }
        else {
            return exclusiveRes;
        }
    }

    /**
     * @brief Calculates the exclusive prefix sum over all workers, given a
     * certain sum operation.
     *
     * @details This method blocks until the sum is caluclated. Values
     * are applied in order, that means sumOp(sumOp(a, b), c) if a, b, c
     * are the values of workers 0, 2, 3.
     *
     * @param value The local value of this worker.
     * @param sumOp The operation to use for
     * calculating the prefix sum. The default operation is a normal addition.
     * @return The prefix sum for the position of this worker.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T ExPrefixSum(const T& value, BinarySumOp sumOp = BinarySumOp()) {
        return PrefixSum(value, sumOp, false);
    }

    /**
     * @brief Broadcasts a value of an integral type T from the master
     * (the worker with id 0) to all other workers.
     * @details This method is blocking on all workers except the master.
     *
     * @param value The value to broadcast. This value is ignored for each
     * worker except the master. We use this signature to keep the decision
     * wether a node is the master transparent.
     * @return The value sent by the master.
     */
    template <typename T>
    T Broadcast(T& value) {

        T res;

        //The primary thread of each node has to handle IO
        if (threadId == 0) {
            SetLocalShared(&res);

            if (id == 0) {
                //Master has to send to all.
                for (int i = 1; i < count; i++) {
                    SendTo(i, value);
                    res = value;
                }
            }
            else {
                //Every other node has to receive.
                ReceiveFrom(0, &res);
            }
        }

        barrier.Await();

        res = *GetLocalShared<T>();

        barrier.Await();

        if (threadId == 0) {
            ClearLocalShared();
        }

        return res;
    }

    /**
     * @brief Reduces a value of an integral type T over all workers given a
     * certain reduce function.
     * @details This method is blocking. The reduce happens in order as with
     * prefix sum. The operation is assumed to be associative.
     *
     * @param value The value to use for the reduce operation.
     * @param sumOp The operation to use for
     * calculating the reduced value. The default operation is a normal addition.
     * @return The result of the reduce operation.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T AllReduce(const T& value, BinarySumOp sumOp = BinarySumOp()) {
        T res = value;
        std::vector<T> localReduceBuffer(threadCount);

        //Local Reduce
        if (threadId == 0) {
            //Master allocate memory.
            localReduceBuffer[threadId] = value;
            SetLocalShared(&localReduceBuffer);
            barrier.Await();
            //Slave store values.
            barrier.Await();

            //Master reduce
            for (int i = 1; i < threadCount; i++) {
                res = sumOp(res, localReduceBuffer[i]);
            }

            //Global Reduce
            //The master receives from evereyone else.
            if (id == 0) {
                T msg;
                for (int i = 1; i < count; i++) {
                    ReceiveFrom(i, &msg);
                    res = sumOp(msg, res);
                }
                //Finally, the result is broadcasted.
                for (int i = 1; i < count; i++) {
                    SendTo(i, res);
                }
            }
            else {
                //Each othe worker just sends the value to the master.
                SendTo(0, res);
                ReceiveFrom(0, &res);
            }

            ClearLocalShared();
            SetLocalShared(&res);
            barrier.Await();
            //Slave get result
            barrier.Await();
            ClearLocalShared();
        }
        else {
            //Master allocate memory.
            barrier.Await();
            //Slave store values.
            (*GetLocalShared<std::vector<T> >())[threadId] = value;
            barrier.Await();
            //Master Reduce
            //Global Reduce
            barrier.Await();
            //Slave get result
            res = *GetLocalShared<T>();
            barrier.Await();
        }

        return res;
    }

    /**
     * @brief A trivial global barrier.
     */
    void Await() {
        int i = 0;
        i = AllReduce(i);
    }
};

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_FLOW_CONTROL_CHANNEL_HEADER

/******************************************************************************/
