/*******************************************************************************
 * c7a/core/job_manager.hpp
 *
 * Part of Project c7a.
 *
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
public:
    JobManager(const std::string& log_prefix = "")
        : flow_manager_(NULL),
          net_dispatcher_(log_prefix + " dm-disp"),
          data_manager_(net_dispatcher_) { }

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

    data::Manager & get_data_manager() {
        return data_manager_;
    }

    net::Manager & get_net_manager() {
        return net_manager_;
    }

    net::FlowControlChannelManager & get_flow_manager() {
        return *flow_manager_;
    }

    size_t get_local_worker_count() {
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
    const static bool debug = false;
    //! number of processing workers on this compute node.
    size_t local_worker_count_;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_JOB_MANAGER_HEADER

/******************************************************************************/
