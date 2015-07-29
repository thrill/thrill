/*******************************************************************************
 * c7a/api/bootstrap.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_BOOTSTRAP_HEADER
#define C7A_API_BOOTSTRAP_HEADER

#include <c7a/api/context.hpp>
#include <c7a/core/job_manager.hpp>

#include <functional>
#include <string>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

//! Executes the given job startpoint with a context instance.
//! Startpoint may be called multiple times with concurrent threads and
//! different context instances.
//!
//! \returns 0 if execution was fine on all threads. Otherwise, the first non-zero return value of any thread is returned.
int Execute(
    int argc, char* const* argv,
    std::function<int(Context&)> job_startpoint,
    size_t local_worker_count = 1, const std::string& log_prefix = "");

/*!
 * Function to run a number of workers as locally independent threads, which
 * still communicate via TCP sockets.
 */
void
ExecuteLocalThreadsTCP(const size_t& workers, const size_t& port_base,
                       std::function<void(Context&)> job_startpoint);

/*!
 * Helper Function to ExecuteLocalThreads in test suite for many different
 * numbers of local workers as independent threads.
 */
void ExecuteLocalTestsTCP(std::function<void(Context&)> job_startpoint);

/*!
 * Function to run a number of mock compute nodes as locally independent
 * threads, which communicate via internal stream sockets.
 */
void
ExecuteLocalMock(size_t node_count, size_t local_worker_count,
                 std::function<void(core::JobManager&, size_t)> job_startpoint);

/*!
 * Helper Function to execute tests using mock networks in test suite for many
 * different numbers of node and workers as independent threads in one program.
 */
void ExecuteLocalTests(std::function<void(Context&)> job_startpoint,
                       const std::string& log_prefix = "");

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_BOOTSTRAP_HEADER

/******************************************************************************/
