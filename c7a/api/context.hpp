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

#include "c7a/data/data_manager.hpp"
#include "c7a/core/job_manager.hpp"

#include <stdio.h>
#include <unistd.h>

namespace c7a {

class Context
{
public:
    Context() : job_manager_() { }
    virtual ~Context() { }

    data::DataManager & get_data_manager() {
        return job_manager_.get_data_manager();
    }

    net::NetGroup & get_flow_net_group() {
        return job_manager_.get_net_manager().GetFlowNetGroup();
    }

    // TODO Off by one? Does size include self-connections
    size_t number_worker() {
        return job_manager_.get_net_manager().Size();
    }

    size_t rank() {
        return job_manager_.get_net_manager().MyRank();
    }

    core::JobManager & job_manager() {
        return job_manager_;
    }

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
