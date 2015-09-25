/*******************************************************************************
 * thrill/net/flow_control_channel.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_FLOW_CONTROL_CHANNEL_HEADER
#define THRILL_NET_FLOW_CONTROL_CHANNEL_HEADER

#include <thrill/common/functional.hpp>
#include <thrill/common/memory.hpp>
#include <thrill/common/thread_barrier.hpp>
#include <thrill/net/collective.hpp>
#include <thrill/net/group.hpp>

#include <functional>
#include <string>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

/*!
 * Provides a blocking collection for communication.
 *
 * This wraps a raw net group, adds multi-worker/thread support, and should be
 * used for flow control with integral types.
 *
 * Important notice on threading: It is not allowed to call two different
 * methods of two different instances of FlowControlChannel simultaniously by
 * different threads, since the internal synchronization state (the barrier) is
 * shared globally.
 *
 * The implementations will be replaced by better/decentral versions.
 *
 * This class is probably the worst thing I've ever coded (ej).
 */
class FlowControlChannel
{
private:
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
    common::AlignedPtr* shmem_;

    template <typename T>
    void SetLocalShared(T* value) {
        // We are only allowed to set our own memory location.
        size_t idx = thread_id_;
        *(reinterpret_cast<T**>(shmem_ + idx)) = value;
    }

    template <typename T>
    T * GetLocalShared(size_t idx) {
        return *(reinterpret_cast<T**>(shmem_ + idx));
    }

    template <typename T>
    T * GetLocalShared() {
        GetLocalShared<T>(thread_id_);
    }

public:
    //! Creates a new instance of this class, wrapping a group.
    explicit FlowControlChannel(Group& group,
                                size_t thread_id, size_t thread_count,
                                common::ThreadBarrier& barrier,
                                common::AlignedPtr* shmem)
        : group_(group),
          id_(group_.my_host_rank()), num_hosts_(group_.num_hosts()),
          thread_id_(thread_id), thread_count_(thread_count),
          barrier_(barrier), shmem_(shmem) { }

    //! Return the associated net::Group. USE AT YOUR OWN RISK.
    Group & group() { return group_; }

    //! Return the worker's global rank
    size_t my_rank() const {
        return group_.my_host_rank() * thread_count_ + thread_id_;
    }

    /*!
     * Calculates the prefix sum over all workers, given a certain sum
     * operation.
     *
     * This method blocks until the sum is caluclated. Values are applied in
     * order, that means sum_op(sum_op(a, b), c) if a, b, c are the values of
     * workers 0, 1, 2.
     *
     * \param value The local value of this worker.
     * \param initial The initial element for the body defined by T and sum_op
     * \param sum_op The operation to use for
     * calculating the prefix sum. The default operation is a normal addition.
     * \param inclusive Whether the prefix sum is inclusive or exclusive.
     * \return The prefix sum for the position of this worker.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T PrefixSum(const T& value, const T& initial = T(),
                BinarySumOp sum_op = BinarySumOp(), bool inclusive = true) {

        static const bool debug = false;

        T local_value = value;

        SetLocalShared(&local_value);

        barrier_.Await();

        // Local Reduce
        if (thread_id_ == 0) {

            // Global Prefix
            T** locals = reinterpret_cast<T**>(alloca(thread_count_ * sizeof(T*)));

            for (size_t i = 0; i < thread_count_; i++) {
                locals[i] = GetLocalShared<T>(i);
            }

            for (size_t i = 1; i < thread_count_; i++) {
                *(locals[i]) = sum_op(*(locals[i - 1]), *(locals[i]));
            }

            if (debug) {
                for (size_t i = 0; i < thread_count_; i++) {
                    LOG << id_ << ", " << i << ", " << inclusive << ": me: " << *(locals[i]);
                }
            }

            T base_sum = *(locals[thread_count_ - 1]);
            group_.PrefixSum(base_sum, sum_op, false);

            if (id_ == 0) {
                base_sum = initial;
            }

            LOG << id_ << ", m, " << inclusive << ": base: " << base_sum;

            if (inclusive) {
                for (size_t i = 0; i < thread_count_; i++) {
                    *(locals[i]) = sum_op(base_sum, *(locals[i]));
                }
            }
            else {
                for (size_t i = thread_count_ - 1; i > 0; i--) {
                    *(locals[i]) = sum_op(base_sum, *(locals[i - 1]));
                }
                *(locals[0]) = base_sum;
            }

            if (debug) {
                for (size_t i = 0; i < thread_count_; i++) {
                    LOG << id_ << ", " << i << ", " << inclusive << ": res: " << *(locals[i]);
                }
            }
        }

        barrier_.Await();

        return local_value;
    }

    /*!
     * Calculates the exclusive prefix sum over all workers, given a certain sum
     * operation.
     *
     * This method blocks until the sum is caluclated. Values are applied in
     * order, that means sum_op(sum_op(a, b), c) if a, b, c are the values of
     * workers 0, 1, 2.
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

    /*!
     * Broadcasts a value of an integral type T from the master (the worker with
     * id 0) to all other workers.
     *
     * This method is blocking on all workers except the master.
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
            res = *GetLocalShared<T>(0);
        }

        barrier_.Await();

        return res;
    }

    /*!
     * Reduces a value of an integral type T over all workers given a certain
     * reduce function.
     *
     * This method is blocking. The reduce happens in order as with prefix
     * sum. The operation is assumed to be associative.
     *
     * \param value The value to use for the reduce operation.
     * \param sum_op The operation to use for
     * calculating the reduced value. The default operation is a normal addition.
     * \return The result of the reduce operation.
     */
    template <typename T, typename BinarySumOp = std::plus<T> >
    T AllReduce(const T& value, BinarySumOp sum_op = BinarySumOp()) {
        T local = value;

        SetLocalShared(&local);

        barrier_.Await();

        // Local Reduce
        if (thread_id_ == 0) {

            // Global reduce
            for (size_t i = 1; i < thread_count_; i++) {
                local = sum_op(local, *GetLocalShared<T>(i));
            }

            group_.AllReduce(local, sum_op);

            // We have the choice: One more barrier so each slave can read
            // from master's shared memory, or p writes to write to each slaves
            // mem. I choose the latter since the cost of a barrier is very high.
            for (size_t i = 1; i < thread_count_; i++) {
                *GetLocalShared<T>(i) = local;
            }
        }

        barrier_.Await();

        return local;
    }

    /*!
     * \brief A trivial global barrier.
     * Use for debugging only.
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
