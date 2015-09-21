/*******************************************************************************
 * thrill/net/flow_control_channel.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_FLOW_CONTROL_CHANNEL_HEADER
#define THRILL_NET_FLOW_CONTROL_CHANNEL_HEADER

#include <thrill/common/functional.hpp>
#include <thrill/common/thread_barrier.hpp>
#include <thrill/net/collective.hpp>
#include <thrill/net/group.hpp>

#include <functional>
#include <string>
#include <vector>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

/**
 * \brief Provides a blocking collection for communication.
 * \details This wraps a raw net group and should be used for
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
    static const bool self_verify = false;

    //! The group associated with this channel.
    Group& group_;

    //! The local id.
    size_t id_;

    //! The count of all workers connected to this group.
    size_t num_hosts_;

    //! The id of the worker thread associated with this flow channel.
    size_t thread_id_;

    //! The count of all workers connected to this group.
    size_t thread_count_;

    //! The shared barrier used to synchronize between worker threads on this node.
    common::ThreadBarrier& barrier_;

    //! A shared memory location to work upon.
    void** shmem_;

    /**
     * \brief Sends a value of an integral type T to a certain other worker.
     * \details This method can block if there is unsufficient space
     * in the send buffer. This method may only be called by thread with ID 0.
     *
     * \param destination The id of the worker to send the value to.
     * \param value The value to send.
     */
    template <typename T>
    void SendTo(size_t destination, const T& value) {

        assert(thread_id_ == 0); //Only primary thread might send/receive.

        group_.connection(destination).Send(value);
    }

    /**
     * \brief Receives a value of an integral type T from a certain other worker.
     * \details This method blocks until the data is received. This method may only be called by thread with ID 0.
     *
     * \param source The id of the worker to receive the value from.
     * \param out_value A pointer to a memory location where the
     * received value is stored.
     */
    template <typename T>
    void ReceiveFrom(size_t source, T* out_value) {

        assert(thread_id_ == 0); // Only primary thread might send/receive.

        group_.connection(source).Receive(out_value);
    }

    template <typename T>
    void SetLocalShared(T* value) {
        if (self_verify)
            assert(*shmem_ == nullptr);
        assert(thread_id_ == 0);
        *shmem_ = value;
    }

    template <typename T>
    T * GetLocalShared() {
        assert(*shmem_ != nullptr);
        return *(reinterpret_cast<T**>(shmem_));
    }

    void ClearLocalShared() {
        if (self_verify) {
            assert(thread_id_ == 0);
            *shmem_ = nullptr;
            barrier_.Await();
        }
    }

public:
    /**
     * \brief Creates a new instance of this class, wrapping a group.
     */
    explicit FlowControlChannel(Group& group,
                                size_t thread_id, size_t thread_count,
                                common::ThreadBarrier& barrier,
                                void** shmem)
        : group_(group),
          id_(group_.my_host_rank()), num_hosts_(group_.num_hosts()),
          thread_id_(thread_id), thread_count_(thread_count),
          barrier_(barrier), shmem_(shmem) { }

    //! Return the associated net::Group. USE AT YOUR OWN RISK.
    Group & group() { return group_; }

    /**
     * \brief Calculates the prefix sum over all workers, given a certain sum
     * operation.
     * \details This method blocks until the sum is caluclated. Values
     * are applied in order, that means sum_op(sum_op(a, b), c) if a, b, c
     * are the values of workers 0, 1, 2.
     *
     * \param value The local value of this worker.
     * \param initial The initial element for the body defined by T and sum_op
     * \param sum_op The operation to use for
     * calculating the prefix sum. The default operation is a normal addition.
     * \param inclusive Whether the prefix sum is inclusive or exclusive.
     * \return The prefix sum for the position of this worker.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T PrefixSum(const T& value, const T& initial = T(), BinarySumOp sum_op = BinarySumOp(),
                bool inclusive = true) {

        static const bool debug = false;

        std::vector<T> localPrefixBuffer(thread_count_);

        // Local Reduce
        if (thread_id_ == 0) {
            // Master allocate memory.
            localPrefixBuffer[thread_id_] = value;
            SetLocalShared(&localPrefixBuffer);
            barrier_.Await();
            // Slave store values.
            barrier_.Await();

            // Global Prefix

            for (size_t i = 1; i < thread_count_; i++) {
                localPrefixBuffer[i] = sum_op(localPrefixBuffer[i - 1], localPrefixBuffer[i]);
            }

            for (size_t i = 0; i < thread_count_; i++) {
                LOG << id_ << ", " << i << ", " << inclusive << ": me: " << localPrefixBuffer[i];
            }

            T prefixSumBase = localPrefixBuffer[thread_count_ - 1];
            group_.PrefixSum(prefixSumBase, sum_op, false);

            if (id_ == 0) {
                prefixSumBase = initial;
            }

            LOG << id_ << ", m, " << inclusive << ": base: " << prefixSumBase;

            if (inclusive) {
                for (size_t i = 0; i < thread_count_; i++) {
                    localPrefixBuffer[i] = sum_op(prefixSumBase, localPrefixBuffer[i]);
                }
            }
            else {
                for (size_t i = thread_count_ - 1; i > 0; i--) {
                    localPrefixBuffer[i] = sum_op(prefixSumBase, localPrefixBuffer[i - 1]);
                }
                localPrefixBuffer[0] = prefixSumBase;
            }

            for (size_t i = 0; i < thread_count_; i++) {
                LOG << id_ << ", " << i << ", " << inclusive << ": res: " << localPrefixBuffer[i];
            }
        }
        else {
            // Master allocate memory.
            barrier_.Await();
            // Slave store values.
            (*GetLocalShared<std::vector<T> >())[thread_id_] = value;
            barrier_.Await();
        }
        barrier_.Await();
        T res = (*GetLocalShared<std::vector<T> >())[thread_id_];
        barrier_.Await();

        return res;
    }

    /**
     * \brief Calculates the exclusive prefix sum over all workers, given a
     * certain sum operation.
     *
     * \details This method blocks until the sum is caluclated. Values
     * are applied in order, that means sum_op(sum_op(a, b), c) if a, b, c
     * are the values of workers 0, 1, 2.
     *
     * \param value The local value of this worker.
     * \param sum_op The operation to use for
     * \param initial The initial element of the body defined by T and SumOp
     * calculating the prefix sum. The default operation is a normal addition.
     * \return The prefix sum for the position of this worker.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T ExPrefixSum(const T& value,
                  const T& initial = T(), BinarySumOp sum_op = BinarySumOp()) {
        return PrefixSum(value, initial, sum_op, false);
    }

    /**
     * \brief Broadcasts a value of an integral type T from the master
     * (the worker with id 0) to all other workers.
     * \details This method is blocking on all workers except the master.
     *
     * \param value The value to broadcast. This value is ignored for each
     * worker except the master. We use this signature to keep the decision
     * wether a node is the master transparent.
     * \return The value sent by the master.
     */
    template <typename T>
    T Broadcast(const T& value) {

        T res = value;

        // The primary thread of each node has to handle IO
        if (thread_id_ == 0) {
            SetLocalShared(&res);

            group_.Broadcast(res);
        }

        barrier_.Await();

        // other threads: read value from thread 0.
        if (thread_id_ != 0) {
            res = *GetLocalShared<T>();
        }

        if (thread_id_ == 0) {
            ClearLocalShared();
        }

        barrier_.Await();

        return res;
    }

    /**
     * \brief Reduces a value of an integral type T over all workers given a
     * certain reduce function.
     * \details This method is blocking. The reduce happens in order as with
     * prefix sum. The operation is assumed to be associative.
     *
     * \param value The value to use for the reduce operation.
     * \param sum_op The operation to use for
     * calculating the reduced value. The default operation is a normal addition.
     * \return The result of the reduce operation.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T AllReduce(const T& value, BinarySumOp sum_op = BinarySumOp()) {
        T res = value;
        std::vector<T> localReduceBuffer(thread_count_);

        // Local Reduce
        if (thread_id_ == 0) {
            // Master allocate memory.
            localReduceBuffer[thread_id_] = value;
            SetLocalShared(&localReduceBuffer);
            barrier_.Await();
            // Slave store values.
            barrier_.Await();

            // Master reduce
            for (size_t i = 1; i < thread_count_; i++) {
                res = sum_op(res, localReduceBuffer[i]);
            }

            group_.AllReduce(res, sum_op);

            ClearLocalShared();
            SetLocalShared(&res);
            barrier_.Await();
            // Slave get result
            ClearLocalShared();
        }
        else {
            // Master allocate memory.
            barrier_.Await();
            // Slave store values.
            (*GetLocalShared<std::vector<T> >())[thread_id_] = value;
            barrier_.Await();
            // Master Reduce
            // Global Reduce
            barrier_.Await();
            // Slave get result
            res = *GetLocalShared<T>();
        }

        barrier_.Await();

        return res;
    }

    /**
     * \brief A trivial global barrier.
     */
    void Barrier() {
        size_t i = 0;
        AllReduce(i);
    }
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_FLOW_CONTROL_CHANNEL_HEADER

/******************************************************************************/
