/*******************************************************************************
 * c7a/core/job_manager.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_JOB_MANAGER_HEADER
#define C7A_CORE_JOB_MANAGER_HEADER

#include <c7a/data/manager.hpp>
#include <c7a/net/manager.hpp>
#include <c7a/net/flow_control_manager.hpp>
#include <c7a/net/dispatcher_thread.hpp>
#include <c7a/common/logger.hpp>

namespace c7a {
namespace core {

class JobManager
{
    const static bool debug = false;

public:
    JobManager(const std::string& log_prefix = "")
        : flow_manager_(NULL),
          net_dispatcher_(log_prefix + " dm-disp"),
          data_manager_(net_dispatcher_)
    { }

    bool Connect(size_t my_rank, const std::vector<net::Endpoint>& endpoints,
                 size_t local_worker_count) {
        local_worker_count_ = local_worker_count;

        net_manager_.Initialize(my_rank, endpoints);
        data_manager_.Connect(&net_manager_.GetDataGroup());
        flow_manager_ = new net::FlowControlChannelManager(
            net_manager_.GetFlowGroup(), local_worker_count_);
        //TODO(??) connect control flow and system control channels here
        return true;
    }

    //! Construct a mock network, consisting of node_count compute nodes, each
    //! with prospective number of local_worker_count. Delivers constructed
    //! JobManager objects internally connected.
    static std::vector<JobManager> ConstructLocalMesh(
        size_t node_count, size_t local_worker_count) {

        // construct list of uninitialized JobManager objects.
        std::vector<JobManager> jm_mesh(node_count);

        // construct mock net::Manager mesh and distribute to JobManager objects
        std::vector<net::Manager> nm_mesh =
            net::Manager::ConstructLocalMesh(node_count);

        for (size_t n = 0; n < node_count; ++n) {
            JobManager& jm = jm_mesh[n];

            // move associated net::Manager
            jm.net_manager_ = std::move(nm_mesh[n]);

            // perform remaining initialization of this JobManager
            jm.local_worker_count_ = local_worker_count;
            jm.data_manager_.Connect(&jm.net_manager_.GetDataGroup());
            jm.flow_manager_ = new net::FlowControlChannelManager(
                jm.net_manager_.GetFlowGroup(), local_worker_count);
        }

        return std::move(jm_mesh);
    }

    data::Manager & data_manager() {
        return data_manager_;
    }

    net::Manager & net_manager() {
        return net_manager_;
    }

    net::FlowControlChannelManager & flow_manager() {
        return *flow_manager_;
    }

    size_t local_worker_count() {
        return local_worker_count_;
    }

    ~JobManager() {
        if (flow_manager_ != NULL) {
            delete flow_manager_;
        }
    }

private:
    net::Manager net_manager_;
    net::FlowControlChannelManager* flow_manager_;
    net::DispatcherThread net_dispatcher_;
    data::Manager data_manager_;
    //! number of processing workers on this compute node.
    size_t local_worker_count_;
};
} // namespace core
} // namespace c7a

#endif // !C7A_CORE_JOB_MANAGER_HEADER

/******************************************************************************/
