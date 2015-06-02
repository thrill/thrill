/*******************************************************************************
 * c7a/api/context.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_CONTEXT_HEADER
#define C7A_API_CONTEXT_HEADER

#include <cassert>
#include <fstream>
#include <string>
#include <vector>

#include <c7a/data/data_manager.hpp>
#include <c7a/core/job_manager.hpp>

#include <stdio.h>
#include <unistd.h>

namespace c7a {

/*!
 * The Context of a job is a unique structure inside a worker, which holds
 *  references to all underlying parts of c7a. The context is able to give
 *  references to the  \ref c7a::data::DataManager "data manager", the
 * \ref c7a::net::NetGroup  "net group" and to the
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

    //! Returns a reference to the net group, which is used to perform network
    //! operations.
    net::Group & get_flow_net_group() {
        return job_manager_.get_net_manager().GetFlowGroup();
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

    //!Returns the current directory.
    std::string get_current_dir() {
        char cCurrentPath[FILENAME_MAX];

        if (!getcwd(cCurrentPath, sizeof(cCurrentPath))) {
            throw "unable to retrieve current directory";
        }

        return getcwd(cCurrentPath, sizeof(cCurrentPath));
    }

private:
    core::JobManager job_manager_;
};

} // namespace c7a

#endif // !C7A_API_CONTEXT_HEADER

/******************************************************************************/
