/*******************************************************************************
 * thrill/api/context.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_CONTEXT_HEADER
#define THRILL_API_CONTEXT_HEADER

#include <thrill/api/stats_graph.hpp>
#include <thrill/common/config.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/stats.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/data/cat_stream.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/mix_stream.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/net/flow_control_channel.hpp>
#include <thrill/net/flow_control_manager.hpp>
#include <thrill/net/manager.hpp>

#include <cassert>
#include <functional>
#include <string>
#include <tuple>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * The HostContext contains all data structures shared among workers on the same
 * host. It is used to construct and destroy them. For testing multiple
 * instances are run in the same process.
 */
class HostContext
{
public:
#if THRILL_HAVE_NET_TCP
    //! Construct one real host connected via TCP to others.
    HostContext(size_t my_host_rank,
                const std::vector<std::string>& endpoints,
                size_t workers_per_host);
#endif

#ifndef SWIG
    //! constructor from existing net Groups. Used by the construction methods.
    HostContext(std::array<net::GroupPtr, net::Manager::kGroupCount>&& groups,
                size_t workers_per_host)
        : workers_per_host_(workers_per_host),
          net_manager_(std::move(groups)),
          flow_manager_(net_manager_.GetFlowGroup(), workers_per_host),
          block_pool_(&mem_manager_, &mem_manager_external_, std::to_string(net_manager_.my_host_rank())),
          data_multiplexer_(mem_manager_,
                            block_pool_, workers_per_host,
                            net_manager_.GetDataGroup())
    { }

    //! Construct a number of mock hosts running in this process.
    static std::vector<std::unique_ptr<HostContext> >
    ConstructLoopback(size_t host_count, size_t workers_per_host);
#endif

    //! number of workers per host (all have the same).
    size_t workers_per_host() const { return workers_per_host_; }

    //! host-global memory manager
    mem::Manager & mem_manager() { return mem_manager_; }

    //! net manager constructs communication groups to other hosts.
    net::Manager & net_manager() { return net_manager_; }

    //! the flow control group is used for collective communication.
    net::FlowControlChannelManager & flow_manager() { return flow_manager_; }

    //! the block manager keeps all data blocks moving through the system.
    data::BlockPool & block_pool() { return block_pool_; }

    //! data multiplexer transmits large amounts of data asynchronously.
    data::Multiplexer & data_multiplexer() { return data_multiplexer_; }

private:
    //! number of workers per host (all have the same).
    size_t workers_per_host_;

    //! host-global memory manager for external memory only
    mem::Manager mem_manager_external_ { nullptr, "HostContext-External" };

    //! host-global memory manager for internal memory only
    mem::Manager mem_manager_ { nullptr, "HostContext" };

    //! net manager constructs communication groups to other hosts.
    net::Manager net_manager_;

    //! the flow control group is used for collective communication.
    net::FlowControlChannelManager flow_manager_;

    //! data block pool
    data::BlockPool block_pool_;

    //! data multiplexer transmits large amounts of data asynchronously.
    data::Multiplexer data_multiplexer_;
};

/*!
 * The Context of a job is a unique instance per worker which holds references
 * to all underlying parts of Thrill. The context is able to give references to
 * the \ref data::Multiplexer "stream multiplexer", the \ref net::Group "net
 * group" \ref common::Stats "stats" and \ref common::StatsGraph "stats graph".
 * Threads share the stream multiplexer and the net group via the context
 * object.
 */
class Context
{
public:
    Context(mem::Manager& mem_manager,
            net::Manager& net_manager,
            net::FlowControlChannelManager& flow_manager,
            data::BlockPool& block_pool,
            data::Multiplexer& multiplexer,
            size_t workers_per_host, size_t local_worker_id)
        : mem_manager_(mem_manager),
          net_manager_(net_manager),
          flow_manager_(flow_manager),
          block_pool_(block_pool),
          multiplexer_(multiplexer),
          local_worker_id_(local_worker_id),
          workers_per_host_(workers_per_host) {
        assert(local_worker_id < workers_per_host);
    }

    Context(HostContext& host_context, size_t local_worker_id)
        : mem_manager_(host_context.mem_manager()),
          net_manager_(host_context.net_manager()),
          flow_manager_(host_context.flow_manager()),
          block_pool_(host_context.block_pool()),
          multiplexer_(host_context.data_multiplexer()),
          local_worker_id_(local_worker_id),
          workers_per_host_(host_context.workers_per_host()) {
        assert(local_worker_id < workers_per_host());
    }

    //! \name System Information
    //! \{

    //! Returns the total number of hosts.
    size_t num_hosts() const {
        return net_manager_.num_hosts();
    }

    //! Returns the number of workers that is hosted on each host
    size_t workers_per_host() const {
        return workers_per_host_;
    }

    //! Global rank of this worker among all other workers in the system.
    size_t my_rank() const {
        return workers_per_host() * host_rank() + local_worker_id();
    }

    //! Global number of workers in the system.
    size_t num_workers() const {
        return num_hosts() * workers_per_host();
    }

    //! Returns id of this host in the cluser
    //! A host is a machine in the cluster that hosts multiple workers
    size_t host_rank() const {
        return net_manager_.my_host_rank();
    }

    //! Returns the local id ot this worker on the host
    //! A worker is _locally_ identified by this id
    size_t local_worker_id() const {
        return local_worker_id_;
    }

#ifndef SWIG
    //! Outputs the context as [host id]:[local worker id] to an std::ostream
    friend std::ostream& operator << (std::ostream& os, const Context& ctx) {
        return os << ctx.host_rank() << ":" << ctx.local_worker_id();
    }
#endif
    //! \}

    //! \name Network Subsystem
    //! \{

    //! Gets the flow control channel for the current worker.
    net::FlowControlChannel & flow_control_channel() {
        return flow_manager_.GetFlowControlChannel(local_worker_id_);
    }

#ifdef SWIG
#define THRILL_ATTRIBUTE_WARN_UNUSED_RESULT
#endif

    //! Broadcasts a value of an integral type T from the master (the worker
    //! with rank 0) to all other workers.
    template <typename T>
    T THRILL_ATTRIBUTE_WARN_UNUSED_RESULT
    Broadcast(const T& value) {
        return flow_control_channel().Broadcast(value);
    }

    //! Reduces a value of an integral type T over all workers given a certain
    //! reduce function.
    template <typename T, typename BinarySumOp = std::plus<T> >
    T THRILL_ATTRIBUTE_WARN_UNUSED_RESULT
    AllReduce(const T& value, const BinarySumOp& sum_op = BinarySumOp()) {
        return flow_control_channel().AllReduce(value, sum_op);
    }

    //! Calculates the prefix sum over all workers, given a certain sum
    //! operation.
    template <typename T, typename BinarySumOp = std::plus<T> >
    T THRILL_ATTRIBUTE_WARN_UNUSED_RESULT
    PrefixSum(const T& value, const T& initial = T(),
              const BinarySumOp& sum_op = BinarySumOp()) {
        return flow_control_channel().PrefixSum(value, initial, sum_op);
    }

    //! Calculates the exclusive prefix sum over all workers, given a certain
    //! sum operation.
    template <typename T, typename BinarySumOp = std::plus<T> >
    T THRILL_ATTRIBUTE_WARN_UNUSED_RESULT
    ExPrefixSum(const T& value, const T& initial = T(),
                const BinarySumOp& sum_op = BinarySumOp()) {
        return flow_control_channel().ExPrefixSum(value, initial, sum_op);
    }

    //! A collective global barrier.
    void Barrier() {
        return flow_control_channel().Barrier();
    }

    //! \}

    //! \name Data Subsystem
    //! \{

    //! Returns a new File object containing a sequence of local Blocks.
    data::File GetFile() {
        return data::File(block_pool_);
    }

    //! Returns a new File, wrapped in a shared_ptr, containing a
    //! sequence of local Blocks.
    data::FilePtr GetFilePtr() {
        return std::make_shared<data::File>(block_pool_);
    }

    //! Returns a reference to a new CatStream. This method alters the state of
    //! the context and must be called on all Workers to ensure correct
    //! communication coordination.
    data::CatStreamPtr GetNewCatStream() {
        return multiplexer_.GetNewCatStream(local_worker_id_);
    }

    //! Returns a reference to a new MixStream. This method alters the state
    //! of the context and must be called on all Workers to ensure correct
    //! communication coordination.
    data::MixStreamPtr GetNewMixStream() {
        return multiplexer_.GetNewMixStream(local_worker_id_);
    }

    //! Returns a reference to a new CatStream or MixStream, selectable via
    //! template parameter.
    template <typename Stream>
    std::shared_ptr<Stream> GetNewStream();

    //! the block manager keeps all data blocks moving through the system.
    data::BlockPool & block_pool() { return block_pool_; }

    //! \}

    //! Returns the stats object for this worker
    common::Stats<common::g_enable_stats> & stats() {
        return stats_;
    }

    //! Returns the stats graph object for this worker
    api::StatsGraph & stats_graph() {
        return stats_graph_;
    }

    //! returns the host-global memory manager
    mem::Manager & mem_manager() { return mem_manager_; }

    //! given a global range [0,global_size) and p PEs to split the range, calculate
    //! the [local_begin,local_end) index range assigned to the PE i. Takes the
    //! information from the Context.
    common::Range CalculateLocalRange(size_t global_size) const {
        return common::CalculateLocalRange(
            global_size, num_workers(), my_rank());
    }

    //! return value of consume flag.
    bool consume() const { return consume_; }

    /*!
     * Sets consume-mode flag such that DIA contents may be consumed during
     * PushData(). When in consume mode the DIA contents is destroyed online
     * when it is transmitted to the next operation. This enables reusing the
     * space of the consume operations. This enabled processing more data with
     * less space. However, by default this mode is DISABLED, because it
     * requires deliberate insertion of .Keep() calls.
     */
    void set_consume(bool consume) { consume_ = consume; }

private:
    //! host-global memory manager
    mem::Manager& mem_manager_;

    //! net::Manager instance that is shared among workers
    net::Manager& net_manager_;

    //! net::FlowControlChannelManager instance that is shared among workers
    net::FlowControlChannelManager& flow_manager_;

    //! data block pool
    data::BlockPool& block_pool_;

    //! data::Multiplexer instance that is shared among workers
    data::Multiplexer& multiplexer_;

    //! StatsGrapg object that is uniquely held for this worker
    api::StatsGraph stats_graph_;
    common::Stats<common::g_enable_stats> stats_;

    //! number of this host context, 0..p-1, within this host
    size_t local_worker_id_;

    //! number of workers hosted per host
    size_t workers_per_host_;

    //! flag to set which enables selective consumption of DIA contents!
    bool consume_ = false;
};

//! \name Run Methods with Internal Networks for Testing
//! \{

/*!
 * Function to run a number of mock hosts as locally independent
 * threads, which communicate via internal stream sockets.
 */
void RunLocalMock(size_t host_count, size_t local_host_count,
                  const std::function<void(Context&)>& job_startpoint);

/*!
 * Helper Function to execute RunLocalMock() tests using mock networks in test
 * suite for many different numbers of workers and hosts as independent threads
 * in one program. Use this function in most test cases.
 */
void RunLocalTests(const std::function<void(Context&)>& job_startpoint);

/*!
 * Runs the given job_startpoint within the same thread with a test network -->
 * run test with one host and one thread.
 */
void RunLocalSameThread(const std::function<void(Context&)>& job_startpoint);

//! \}

/*!
 * Runs the given job startpoint with a Context instance.  Startpoints may be
 * called multiple times with concurrent threads and different context instances
 * across different workers.  The Thrill configuration is taken from environment
 * variables starting the THRILL_.
 *
 * THRILL_NET is the network backend to use, e.g.: mock, local, tcp, or mpi.
 *
 * THRILL_RANK contains the rank of this worker
 *
 * THRILL_HOSTLIST contains a space- or comma-separated list of host:ports to
 * connect to.
 *
 * THRILL_WORKERS_PER_HOST is the number of workers (threads) per host.
 *
 * Additional variables:
 *
 * THRILL_DIE_WITH_PARENT sets a flag which terminates the program if the caller
 * terminates (this is automatically set by ssh/invoke.sh). No more zombies.
 *
 * THRILL_UNLINK_BINARY deletes a file. Used by ssh/invoke.sh to unlink a copied
 * program binary while it is running. Hence, it can keep /tmp clean.
 *
 * \returns 0 if execution was fine on all threads.
 */
int Run(const std::function<void(Context&)>& job_startpoint);

//! \}

} // namespace api

//! imported from api namespace
using api::HostContext;

//! imported from api namespace
using api::Context;

//! imported from api namespace
using api::Run;

} // namespace thrill

#endif // !THRILL_API_CONTEXT_HEADER

/******************************************************************************/
