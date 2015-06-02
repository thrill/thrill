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
        : cmp_(net_dispatcher_.dispatcher()), data_manager_(cmp_),
          dispatcher_running_(false) { }

    bool Connect(size_t my_rank, const std::vector<net::Endpoint>& endpoints) {
        net_manager_.Initialize(my_rank, endpoints);
        cmp_.Connect(&net_manager_.GetDataGroup());
        //TODO(??) connect control flow and system control channels here
        return true;
    }

    data::Manager & get_data_manager() {
        return data_manager_;
    }

    net::Manager & get_net_manager() {
        return net_manager_;
    }

    //! Starts the dispatcher thread of the Manager
    //! \throws std::runtime_exception if the thread is already running
    void StartDispatcher() {
        net_dispatcher_.Start();
    }

    //! Stops the dispatcher thread of the Manager
    void StopDispatcher() {
        net_dispatcher_.Stop();
    }

private:
    net::Manager net_manager_;
    net::DispatcherThread net_dispatcher_;
    net::ChannelMultiplexer cmp_;
    data::Manager data_manager_;
    bool dispatcher_running_;
    std::thread dispatcher_thread_;
    const static bool debug = true;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_JOB_MANAGER_HEADER

/******************************************************************************/
