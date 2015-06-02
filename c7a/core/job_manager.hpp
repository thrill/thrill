/*******************************************************************************
 * c7a/core/job_manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_JOB_MANAGER_HEADER
#define C7A_CORE_JOB_MANAGER_HEADER

#include <c7a/data/data_manager.hpp>
#include <c7a/net/net_manager.hpp>
#include <c7a/common/logger.hpp>

//includes for thread and condition variables magic
#include <thread>
#include <mutex>
#include <condition_variable>

namespace c7a {
namespace core {

class JobManager
{
public:
    JobManager() : net_manager_(), net_dispatcher_(), cmp_(net_dispatcher_), data_manager_(cmp_), dispatcher_running_(false) { }

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
        LOG << "starting net dispatcher";
        dispatcher_thread_ = std::thread([=]() { this->net_dispatcher_.DispatchLoop(); });
        dispatcher_running_ = true;
    }

    //! Stops the dispatcher thread of the Manager
    void StopDispatcher() {
        LOG << "stopping dispatcher ... waiting for it's breakout";
        net_dispatcher_.Breakout();
        dispatcher_thread_.join();
        dispatcher_running_ = false;
        LOG << "dispatcher thread joined";
    }

private:
    net::Manager net_manager_;
    net::Dispatcher net_dispatcher_;
    net::ChannelMultiplexer cmp_;
    data::Manager data_manager_;
    std::mutex waiting_on_data_;
    std::condition_variable idontknowhowtonameit_;
    bool new_data_arrived_;
    bool dispatcher_running_;
    std::thread dispatcher_thread_;
    const static bool debug = true;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_JOB_MANAGER_HEADER

/******************************************************************************/
