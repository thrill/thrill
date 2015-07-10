/*******************************************************************************
 * c7a/api/bootstrap.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_BOOTSTRAP_HEADER
#define C7A_API_BOOTSTRAP_HEADER

#include <c7a/api/context.hpp>
#include <c7a/core/job_manager.hpp>
#include <c7a/common/stats_timer.hpp>
#include <c7a/common/cmdline_parser.hpp>
#include <c7a/common/logger.hpp>

#include <tuple>
#include <thread>
#include <atomic>
#include <random>

namespace c7a {
namespace api {

namespace {

std::tuple<int, size_t, std::vector<std::string> >
ParseArgs(int argc, char* const* argv) {
    //replace with arbitrary complex implementation
    size_t my_rank;
    std::vector<std::string> endpoints;
    c7a::common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    unsigned int rank = 1;
    clp.AddUInt('r', "rank", "R", rank,
                "Rank of this worker");

    std::vector<std::string> addr;
    clp.AddParamStringlist("addresses", addr,
                           "List of all worker addresses.");

    if (!clp.Process(argc, argv)) {
        return std::make_tuple(-1, my_rank, endpoints);
    }

    for (auto address : addr) {
        if (address.find(":") == std::string::npos) {
            std::cerr << "Invalid address. No Portnumber detectable";
            return std::make_tuple(-1, my_rank, endpoints);
        }
    }

    if (argc > 2) {
        my_rank = rank;
        endpoints.assign(addr.begin(), addr.end());
    }
    else if (argc == 2) {
        std::cerr << "Wrong number of arguments. Must be 0 or > 1";
        return std::make_tuple(-1, my_rank, endpoints);
    }
    else {
        my_rank = 0;
        endpoints.push_back("127.0.0.1:1234");
    }
    return std::make_tuple(0, my_rank, endpoints);
}

} // namespace

//! Executes the given job startpoint with a context instance.
//! Startpoint may be called multiple times with concurrent threads and
//! different context instances.
//!
//! \returns 0 if execution was fine on all threads. Otherwise, the first non-zero return value of any thread is returned.
static inline int Execute(
    int argc, char* const* argv,
    std::function<int(Context&)> job_startpoint,
    size_t thread_count = 1, const std::string& log_prefix = "") {

    //!True if program time should be taken and printed

    static const bool debug = false;

    size_t my_rank;
    std::vector<std::string> endpoints;
    int result = 0;
    std::tie(result, my_rank, endpoints) = ParseArgs(argc, argv);
    if (result != 0)
        return -1;

    if (my_rank >= endpoints.size()) {
        std::cerr << "endpoint list (" <<
            endpoints.size() <<
            " entries) does not include my rank (" <<
            my_rank << ")" << std::endl;
        return -1;
    }

    LOG << "executing " << argv[0] << " with rank " << my_rank << " and endpoints";
    for (const auto& ep : endpoints)
        LOG << ep << " ";

    core::JobManager jobMan(log_prefix);
    jobMan.Connect(my_rank, net::Endpoint::ParseEndpointList(endpoints), thread_count);

    std::vector<std::thread> threads(thread_count);
    std::vector<std::atomic<int> > atomic_results(thread_count);

    for (size_t i = 0; i < thread_count; i++) {
        threads[i] = std::thread(
            [&jobMan, &atomic_results, &job_startpoint, i, log_prefix] {
                Context ctx(jobMan, i);
                common::ThreadDirectory.NameThisThread(log_prefix + " thread " + std::to_string(i));
                LOG << "connecting to peers";
                LOG << "Starting job on Worker " << ctx.rank();
                auto overall_timer = ctx.get_stats().CreateTimer("job::overall", "", true);
                int job_result = job_startpoint(ctx);
                overall_timer->Stop();
                LOG << "Worker " << ctx.rank() << " done!";
                atomic_results[i] = job_result;
                jobMan.get_flow_manager().GetFlowControlChannel(0).await();
            });
    }
    for (size_t i = 0; i < thread_count; i++) {
        threads[i].join();
        if (atomic_results[i] != 0 && result == 0)
            result = atomic_results[i];
    }
    return result;
}

/*!
 * Function to run a number of workers as locally independent threads, which
 * still communicate via TCP sockets.
 */
static inline void
ExecuteLocalThreads(const size_t& workers, const size_t& port_base,
                    std::function<void(Context&)> job_startpoint) {

    std::vector<std::thread> threads(workers);
    std::vector<std::vector<std::string> > strargs(workers);
    std::vector<std::vector<char*> > args(workers);

    for (size_t i = 0; i < workers; i++) {

        // construct command line for independent worker thread

        strargs[i] = { "local_c7a", "-r", std::to_string(i) };

        for (size_t j = 0; j < workers; j++)
            strargs[i].push_back("127.0.0.1:" + std::to_string(port_base + j));

        // make a char*[] array from std::string array (argv compatible)

        args[i].resize(strargs[i].size() + 1);
        for (size_t j = 0; j != strargs[i].size(); ++j) {
            args[i][j] = const_cast<char*>(strargs[i][j].c_str());
        }
        args[i].back() = NULL;

        std::function<int(Context&)> intReturningFunction =
            [job_startpoint](Context& ctx) {
                job_startpoint(ctx);
                return 1;
            };

        threads[i] = std::thread(
            [=]() {
                Execute(strargs[i].size(), args[i].data(),
                        intReturningFunction, 1, "worker " + std::to_string(i));
            });
    }

    for (size_t i = 0; i < workers; i++) {
        threads[i].join();
    }
}

/*!
 * Helper Function to ExecuteLocalThreads in test suite for many different
 * numbers of local workers as independent threads.
 */
static inline void
ExecuteLocalTests(std::function<void(Context&)> job_startpoint) {

    // randomize base port number for test
    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(30000, 65000);
    const size_t port_base = distribution(generator);

    for (size_t workers = 1; workers <= 8; workers *= 2) {
        ExecuteLocalThreads(workers, port_base, job_startpoint);
    }
}

} // namespace api
} // namespace c7a

#endif // !C7A_API_BOOTSTRAP_HEADER

/******************************************************************************/
