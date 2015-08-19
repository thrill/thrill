/*******************************************************************************
 * c7a/api/context.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_CONTEXT_HEADER
#define C7A_API_CONTEXT_HEADER

#include <c7a/api/stats_graph.hpp>
#include <c7a/common/config.hpp>
#include <c7a/common/stats.hpp>
#include <c7a/data/channel.hpp>
#include <c7a/data/file.hpp>
#include <c7a/data/multiplexer.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>
#include <c7a/net/manager.hpp>

#include <cassert>
#include <cstdio>
#include <fstream>
#include <functional>
#include <string>
#include <vector>

namespace c7a {
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
    HostContext(size_t my_host_rank,
                const std::vector<std::string>& endpoints,
                size_t workers_per_host)
        : workers_per_host_(workers_per_host),
          net_manager_(my_host_rank, endpoints),
          flow_manager_(net_manager_.GetFlowGroup(), workers_per_host),
          data_multiplexer_(workers_per_host, net_manager_.GetDataGroup())
    { }

#ifndef SWIG
    //! constructor from existing net Groups for use from ConstructLocalMock().
    HostContext(size_t my_host_rank,
                std::array<net::Group, net::Manager::kGroupCount>&& groups,
                size_t workers_per_host)
        : workers_per_host_(workers_per_host),
          net_manager_(my_host_rank, std::move(groups)),
          flow_manager_(net_manager_.GetFlowGroup(), workers_per_host),
          data_multiplexer_(workers_per_host, net_manager_.GetDataGroup())
    { }

    //! Construct a number of mock hosts running in this process.
    static std::vector<std::unique_ptr<HostContext> >
    ConstructLocalMock(size_t host_count, size_t workers_per_host);
#endif

    //! number of workers per host (all have the same).
    size_t workers_per_host() const { return workers_per_host_; }

    //! net manager constructs communication groups to other hosts.
    net::Manager & net_manager() { return net_manager_; }

    //! the flow control group is used for collective communication.
    net::FlowControlChannelManager & flow_manager() { return flow_manager_; }

    //! data multiplexer transmits large amounts of data asynchronously.
    data::Multiplexer & data_multiplexer() { return data_multiplexer_; }

protected:
    //! number of workers per host (all have the same).
    size_t workers_per_host_;

    //! net manager constructs communication groups to other hosts.
    net::Manager net_manager_;

    //! the flow control group is used for collective communication.
    net::FlowControlChannelManager flow_manager_;

    //! data multiplexer transmits large amounts of data asynchronously.
    data::Multiplexer data_multiplexer_;
};

/*!
 * The Context of a job is a unique instance per worker which holds
 *  references to all underlying parts of c7a. The context is able to give
 *  references to the  \ref c7a::data::Multiplexer "channel multiplexer", the
 * \ref c7a::net::Group  "net group"
 * \ref c7a::common::Stats "stats" and
 * \ref c7a::common::StatsGraph "stats graph".
 * Threads share the channel multiplexer and
 * the net group via the context object.
 */
class Context
{
public:
    Context(net::Manager& net_manager,
            net::FlowControlChannelManager& flow_manager,
            data::Multiplexer& multiplexer,
            size_t workers_per_host, size_t local_worker_id)
        : net_manager_(net_manager),
          flow_manager_(flow_manager),
          multiplexer_(multiplexer),
          local_worker_id_(local_worker_id),
          workers_per_host_(workers_per_host) {
        assert(local_worker_id < workers_per_host);
    }

    Context(HostContext& host_context, size_t local_worker_id)
        : net_manager_(host_context.net_manager()),
          flow_manager_(host_context.flow_manager()),
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

    //! \}

    //! \name Network Subsystem
    //! \{

    /**
     * \brief Gets the flow control channel for the current worker.
     *
     * \return The flow control channel instance for this worker.
     */
    net::FlowControlChannel & flow_control_channel() {
        return flow_manager_.GetFlowControlChannel(local_worker_id_);
    }

    //! Broadcasts a value of an integral type T from the master (the worker
    //! with rank 0) to all other workers.
    template <typename T>
    T Broadcast(const T& value) {
        return flow_control_channel().Broadcast(value);
    }

    //! Reduces a value of an integral type T over all workers given a certain
    //! reduce function.
    template <typename T, typename BinarySumOp = std::plus<T> >
    T AllReduce(const T& value, BinarySumOp sumOp = BinarySumOp()) {
        return flow_control_channel().AllReduce(value, sumOp);
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
        return data::File();
    }

    //! Returns a reference to a new Channel.  This method alters the state of
    //! the context and must be called on all Workers to ensure correct
    //! communication coordination.
    data::ChannelPtr GetNewChannel() {
        return std::move(multiplexer_.GetNewChannel(local_worker_id_));
    }

    //! \}

    //! Returns the stas object for this worker
    common::Stats<common::g_enable_stats> & stats() {
        return stats_;
    }

    //! Returns the stats graph object for this worker
    api::StatsGraph & stats_graph() {
        return stats_graph_;
    }

private:
    //! net::Manager instance that is shared among workers
    net::Manager& net_manager_;

    //! net::FlowControlChannelManager instance that is shared among workers
    net::FlowControlChannelManager& flow_manager_;

    //! data::Multiplexer instance that is shared among workers
    data::Multiplexer& multiplexer_;

    //! StatsGrapg object that is uniquely held for this worker
    api::StatsGraph stats_graph_;
    common::Stats<common::g_enable_stats> stats_;

    //! number of this host context, 0..p-1, within this host
    size_t local_worker_id_;

    //! number of workers hosted per host
    size_t workers_per_host_;
};

//! Outputs the context as [host id]:[local worker id] to an std::ostream
static inline std::ostream& operator << (std::ostream& os, const Context& ctx) {
    return os << ctx.host_rank() << ":" << ctx.local_worker_id();
}

/*!
 * Function to run a number of mock hosts as locally independent
 * threads, which communicate via internal stream sockets.
 */
void
RunLocalMock(size_t host_count, size_t local_host_count,
             std::function<void(api::Context&)> job_startpoint);

/*!
 * Helper Function to execute tests using mock networks in test suite for many
 * different numbers of workers and hosts as independent threads in one program.
 */
void RunLocalTests(std::function<void(Context&)> job_startpoint);

/*!
 * Runs the given job_startpoint within the same thread -->
 * one host with one thread
 */
void RunSameThread(std::function<void(Context&)> job_startpoint);

/*!
 * Runs the given job startpoint with a context instance.  Startpoints may
 * be called multiple times with concurrent threads and different context
 * instances across different workers.  The c7a configuration is taken from
 * environment variables starting the C7A_.
 *
 * C7A_RANK contains the rank of this worker
 *
 * C7A_HOSTLIST contains a space- or comma-separated list of host:ports to connect to.
 *
 * \returns 0 if execution was fine on all threads. Otherwise, the first
 * non-zero return value of any thread is returned.
 */
int Run(
    std::function<void(Context&)> job_startpoint,
    const std::string& log_prefix = std::string());

//! \}

} // namespace api

//! imported from api namespace
using c7a::api::HostContext;
using c7a::api::Context;

} // namespace c7a

#endif // !C7A_API_CONTEXT_HEADER

/******************************************************************************/
