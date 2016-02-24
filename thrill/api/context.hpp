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

#include <thrill/common/config.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/json_logger.hpp>
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

class MemoryConfig
{
public:
    int setup_detect();
    void setup_test();

    MemoryConfig divide(size_t hosts) const;
    void apply();

    void print(size_t workers_per_host) const;

    //! total amount of physical ram detected or THRILL_RAM
    size_t ram_;

    //! amount of RAM dedicated to data::BlockPool -- hard limit
    size_t ram_block_pool_hard_;

    //! amount of RAM dedicated to data::BlockPool -- soft limit
    size_t ram_block_pool_soft_;

    //! total amount of RAM for DIANode data structures such as the reduce
    //! tables. divide by the number of worker threads before use.
    size_t ram_workers_;

    //! remaining free-floating RAM used for user and Thrill data structures.
    size_t ram_floating_;
};

/*!
 * The HostContext contains all data structures shared among workers on the same
 * host. It is used to construct and destroy them. For testing multiple
 * instances are run in the same process.
 */
class HostContext
{
public:
#ifndef SWIG
    //! constructor from existing net Groups. Used by the construction methods.
    HostContext(const MemoryConfig& mem_config,
                std::array<net::GroupPtr, net::Manager::kGroupCount>&& groups,
                size_t workers_per_host)
        : base_logger_(MakeHostLogPath(groups[0]->my_host_rank())),
          logger_(&base_logger_, "host_rank", groups[0]->my_host_rank()),
          mem_config_(mem_config),
          workers_per_host_(workers_per_host),
          net_manager_(std::move(groups))
    { }

    //! Construct a number of mock hosts running in this process.
    static std::vector<std::unique_ptr<HostContext> >
    ConstructLoopback(size_t num_hosts, size_t workers_per_host);
#endif

    //! create host log
    static std::string MakeHostLogPath(size_t worker_rank);

    //! number of workers per host (all have the same).
    size_t workers_per_host() const { return workers_per_host_; }

    //! memory limit of each worker Context for local data structures
    size_t worker_mem_limit() const {
        return mem_config_.ram_workers_ / workers_per_host_;
    }

    //! host-global memory manager
    mem::Manager & mem_manager() { return mem_manager_; }

    //! net manager constructs communication groups to other hosts.
    net::Manager & net_manager() { return net_manager_; }

    //! Returns id of this host in the cluser. A host is a machine in the
    //! cluster that hosts multiple workers
    size_t host_rank() const { return net_manager_.my_host_rank(); }

    //! the flow control group is used for collective communication.
    net::FlowControlChannelManager & flow_manager() { return flow_manager_; }

    //! the block manager keeps all data blocks moving through the system.
    data::BlockPool & block_pool() { return block_pool_; }

    //! data multiplexer transmits large amounts of data asynchronously.
    data::Multiplexer & data_multiplexer() { return data_multiplexer_; }

public:
    //! \name Logging System
    //! {

    //! base logger exclusive for this host context
    common::JsonLogger base_logger_;

    //! public member which delivers key:value pairs as JSON log lines. this
    //! logger is local to this Context which is exclusive for one worker
    //! thread.
    common::JsonLogger logger_;

    //! }

private:
    //! memory configuration
    MemoryConfig mem_config_;

    //! number of workers per host (all have the same).
    size_t workers_per_host_;

    //! host-global memory manager for internal memory only
    mem::Manager mem_manager_ { nullptr, "HostContext" };

    //! net manager constructs communication groups to other hosts.
    net::Manager net_manager_;

    //! the flow control group is used for collective communication.
    net::FlowControlChannelManager flow_manager_ {
        net_manager_.GetFlowGroup(), workers_per_host_
    };

    //! data block pool
    data::BlockPool block_pool_ {
        mem_config_.ram_block_pool_soft_, mem_config_.ram_block_pool_hard_,
        &logger_, &mem_manager_, workers_per_host_
    };

    //! data multiplexer transmits large amounts of data asynchronously.
    data::Multiplexer data_multiplexer_ {
        mem_manager_, block_pool_, workers_per_host_,
        net_manager_.GetDataGroup()
    };
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
            size_t workers_per_host, size_t local_worker_id,
            size_t mem_limit)
        : local_worker_id_(local_worker_id),
          workers_per_host_(workers_per_host),
          mem_limit_(mem_limit),
          mem_manager_(mem_manager),
          net_manager_(net_manager),
          flow_manager_(flow_manager),
          block_pool_(block_pool),
          multiplexer_(multiplexer),
          base_logger_(MakeWorkerLogPath(my_rank())) {
        assert(local_worker_id < workers_per_host);
    }

    Context(HostContext& host_context, size_t local_worker_id)
        : local_worker_id_(local_worker_id),
          workers_per_host_(host_context.workers_per_host()),
          mem_limit_(host_context.worker_mem_limit()),
          mem_manager_(host_context.mem_manager()),
          net_manager_(host_context.net_manager()),
          flow_manager_(host_context.flow_manager()),
          block_pool_(host_context.block_pool()),
          multiplexer_(host_context.data_multiplexer()),
          base_logger_(MakeWorkerLogPath(my_rank())) {
        assert(local_worker_id < workers_per_host());
    }

    //! create worker log
    static std::string MakeWorkerLogPath(size_t worker_rank);

    //! method used to launch a job's main procedure. it wraps it in log output.
    void Launch(const std::function<void(Context&)>& job_startpoint);

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

    //! memory limit of this worker Context for local data structures
    size_t mem_limit() const { return mem_limit_; }

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

    //! \name Data Subsystem
    //! \{

    //! Returns a new File object containing a sequence of local Blocks.
    data::File GetFile() {
        return data::File(block_pool_, local_worker_id_);
    }

    //! Returns a new File, wrapped in a shared_ptr, containing a
    //! sequence of local Blocks.
    data::FilePtr GetFilePtr() {
        return std::make_shared<data::File>(block_pool_, local_worker_id_);
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
    void enable_consume(bool consume = true) { consume_ = consume; }

    //! Returns next_dia_id_ to generate DIA::id_ serial.
    size_t next_dia_id() { return ++last_dia_id_; }

private:
    //! number of this host context, 0..p-1, within this host
    size_t local_worker_id_;

    //! number of workers hosted per host
    size_t workers_per_host_;

    //! memory limit of this worker Context for local data structures
    size_t mem_limit_;

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

    //! flag to set which enables selective consumption of DIA contents!
    bool consume_ = false;

    //! the number of valid DIA ids. 0 is reserved for invalid.
    size_t last_dia_id_ = 0;

public:
    //! \name Network Subsystem
    //! \{

    //! public member which exposes all network primitives from
    //! FlowControlChannel for DOp implementations. Use it as
    //! `context_.net.Method()`.
    net::FlowControlChannel& net {
        flow_manager_.GetFlowControlChannel(local_worker_id_)
    };

    //! \}

public:
    //! \name Logging System
    //! {

    //! base logger exclusive for this worker
    common::JsonLogger base_logger_;

    //! public member which delivers key:value pairs as JSON log lines. this
    //! logger is local to this Context which is exclusive for one worker
    //! thread.
    common::JsonLogger logger_ {
        &base_logger_, "host_rank", host_rank(), "worker_rank", my_rank()
    };

    //! }
};

//! \name Run Methods with Internal Networks for Testing
//! \{

/*!
 * Function to run a number of mock hosts as locally independent threads, which
 * communicate via internal stream sockets.
 */
void RunLocalMock(size_t num_hosts, size_t workers_per_host,
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
