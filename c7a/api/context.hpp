/*******************************************************************************
 * c7a/api/context.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_CONTEXT_HEADER
#define C7A_API_CONTEXT_HEADER

#include <cassert>
#include <fstream>
#include <string>
#include <vector>

#include <c7a/data/manager.hpp>
#include <c7a/core/job_manager.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>
#include "c7a/common/stats.hpp"

#include <stdio.h>
#include <unistd.h>

namespace c7a {

/*!
 * The Context of a job is a unique structure inside a worker, which holds
 *  references to all underlying parts of c7a. The context is able to give
 *  references to the  \ref c7a::data::Manager "data manager", the
 * \ref c7a::net::Group  "net group" and to the
 * \ref c7a::core::JobManager "job manager". The context can also return the
 * total number of workers and the rank of this worker.
 */
class Context
{
public:
    Context() : job_manager_() { }
    virtual ~Context() { }

    //! Returns a reference to the data manager, which gives iterators and
    //! emitters for data.
    data::Manager & get_data_manager() {
        return job_manager_.get_data_manager();
    }

    // This is forbidden now. Muha. >) (ej)
    //net::Group & get_flow_net_group() {
    //   return job_manager_.get_net_manager().GetFlowGroup();
    //}

    /**
     * @brief Gets the flow control channel for a certain thread.
     * 
     * @param threadId The ID of the thread to get the flow channel for. 
     * @return The flow control channel associated with the given ID. 
     */
    net::FlowControlChannel & get_flow_control_channel(int threadId) {
        return job_manager_.get_flow_manager().GetFlowControlChannel(threadId);
    }

    //! Returns the total number of workers.
    size_t number_worker() {
        return job_manager_.get_net_manager().Size();
    }

    //!Returns the rank of this worker. Between 0 and number_worker() - 1
    size_t rank() {
        return job_manager_.get_net_manager().MyRank();
    }

    //!Returns a reference to the job manager, which handles the dispatching
    //! of messages.
    core::JobManager & job_manager() {
        return job_manager_;
    }

    common::Stats & get_stats() {
        return stats_;
    }

private:
    core::JobManager job_manager_;
    common::Stats stats_;
};

} // namespace c7a

#endif // !C7A_API_CONTEXT_HEADER

/******************************************************************************/
