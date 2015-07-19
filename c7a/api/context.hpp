/*******************************************************************************
 * c7a/api/context.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_CONTEXT_HEADER
#define C7A_API_CONTEXT_HEADER

#include <c7a/data/manager.hpp>
#include <c7a/core/job_manager.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>
#include <c7a/common/stats.hpp>

#include <cassert>
#include <fstream>
#include <string>
#include <vector>
#include <cstdio>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

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
    Context(core::JobManager& job_manager, int local_worker_id)
        : job_manager_(job_manager), local_worker_id_(local_worker_id) { }

    //! Returns a reference to the data manager, which gives iterators and
    //! emitters for data.
    data::Manager & data_manager() {
        if (local_worker_id_ != 0)
        {
            //TODO (ts)
            assert(false && "Data Manager does not support multi-threading at the moment.");
        }
        return job_manager_.data_manager();
    }

    // This is forbidden now. Muha. >) (ej)
    //net::Group & flow_net_group() {
    //   return job_manager_.net_manager().GetFlowGroup();
    //}

    /**
     * @brief Gets the flow control channel for a certain thread.
     *
     * @return The flow control channel associated with the given ID.
     */
    net::FlowControlChannel & flow_control_channel() {
        return job_manager_.flow_manager().GetFlowControlChannel(local_worker_id_);
    }

    //! Returns the total number of workers.
    size_t number_worker() {
        return job_manager_.net_manager().Size();
    }

    //! Returns the rank of this worker. Between 0 and number_worker() - 1
    size_t rank() {
        return job_manager_.net_manager().MyRank();
    }

    common::Stats & stats() {
        return stats_;
    }

    int local_worker_id() {
        return local_worker_id_;
    }

    int local_worker_count() {
        return job_manager_.local_worker_count();
    }

private:
    core::JobManager& job_manager_;
    common::Stats stats_;

    //! number of this worker context, 0..p-1, within this compute node.
    int local_worker_id_;
};

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_CONTEXT_HEADER

/******************************************************************************/
