/*******************************************************************************
 * c7a/api/context.hpp
 *
 * Part of Project c7a.
 *
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

#include "c7a/data/data_manager.hpp"
#include "c7a/core/job_manager.hpp"

namespace c7a {

class Context
{
public:
    Context() : job_manager_() { }
    virtual ~Context() { }

    data::DataManager & get_data_manager() {
        return job_manager_.get_data_manager();
    }
    int number_worker() {
        return number_worker_;
    }

    core::JobManager & GetJobManager() {
        return job_manager_;
    }

private:
    core::JobManager job_manager_;
    //stub
    int number_worker_ = 1;
};

} // namespace c7a

#endif // !C7A_API_CONTEXT_HEADER

/******************************************************************************/
