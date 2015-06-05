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
#include <c7a/net/dispatcher_thread.hpp>
#include <c7a/common/logger.hpp>

namespace c7a {
namespace core {

class JobManager
{
public:
    JobManager()
        : data_manager_(net_dispatcher_) { }

    bool Connect(size_t my_rank, const std::vector<net::Endpoint>& endpoints) {
        net_manager_.Initialize(my_rank, endpoints);
        data_manager_.Connect(&net_manager_.GetDataGroup());
        //TODO(??) connect control flow and system control channels here
        return true;
    }

    data::Manager & get_data_manager() {
        return data_manager_;
    }

    net::Manager & get_net_manager() {
        return net_manager_;
    }

private:
    net::Manager net_manager_;
    net::DispatcherThread net_dispatcher_;
    data::Manager data_manager_;
    const static bool debug = false;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_JOB_MANAGER_HEADER

/******************************************************************************/
