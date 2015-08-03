/*******************************************************************************
 * c7a/api/context.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/context.hpp>

#include <c7a/common/cmdline_parser.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/common/stats.hpp>

#include <atomic>
#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

namespace c7a {
namespace api {

static inline
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

//! Executes the given job startpoint with a context instance.
//! Startpoint may be called multiple times with concurrent threads and
//! different context instances.
//!
//! \returns 0 if execution was fine on all threads. Otherwise, the first non-zero return value of any thread is returned.
int Execute(
    int argc, char* const* argv,
    std::function<void(Context&)> job_startpoint,
    size_t local_worker_count, const std::string& log_prefix) {

    // true if program time should be taken and printed
    static const bool debug = false;

    size_t my_rank;
    std::vector<std::string> endpoints;
    int global_result = 0;
    std::tie(global_result, my_rank, endpoints) = ParseArgs(argc, argv);
    if (global_result != 0)
        return -1;

    if (my_rank >= endpoints.size()) {
        std::cerr << "endpoint list (" << endpoints.size() << " entries) "
                  << "does not include my rank (" << my_rank << ")"
                  << std::endl;
        return -1;
    }

    LOG << "executing " << argv[0] << " with rank " << my_rank << " and endpoints";
    for (const std::string& ep : endpoints)
        LOG << ep << " ";

    // construct node global objects: net::Manager, data::Manager, etc.

    core::JobManager jobMan(log_prefix);
    jobMan.Connect(my_rank, net::Endpoint::ParseEndpointList(endpoints),
                   local_worker_count);

    // launch initial thread for each of the workers on this node.

    std::vector<std::thread> threads(local_worker_count);

    for (size_t i = 0; i < local_worker_count; i++) {
        threads[i] = std::thread(
            [&jobMan, &job_startpoint, i, log_prefix] {
                Context ctx(jobMan, i);
                common::NameThisThread(
                    log_prefix + " worker " + std::to_string(i));

                LOG << "Starting job on worker " << ctx.rank();
                auto overall_timer = ctx.stats().CreateTimer("job::overall", "", true);
                // TODO: this cannot be correct, the job needs to know which
                // worker number it is on the node.
                job_startpoint(ctx);
                STOP_TIMER(overall_timer)
                LOG << "Worker " << ctx.rank() << " done!";

                jobMan.flow_manager().GetFlowControlChannel(0).Await();
            });
    }

    // join worker threads

    for (size_t i = 0; i < local_worker_count; i++) {
        threads[i].join();
    }

    return global_result;
}

/*!
 * Function to run a number of workers as locally independent threads, which
 * still communicate via TCP sockets.
 */
void
ExecuteLocalThreadsTCP(const size_t& workers, const size_t& port_base,
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

        threads[i] = std::thread(
            [=]() {
                Execute(strargs[i].size(), args[i].data(),
                        job_startpoint, 1, "worker " + std::to_string(i));
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
void ExecuteLocalTestsTCP(std::function<void(Context&)> job_startpoint) {

    // randomize base port number for test
    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(10000, 30000);
    const size_t port_base = distribution(generator);

    for (size_t workers = 1; workers <= 8; workers *= 2) {
        ExecuteLocalThreadsTCP(workers, port_base, job_startpoint);
    }
}

/*!
 * Function to run a number of mock compute nodes as locally independent
 * threads, which communicate via internal stream sockets.
 */
void
ExecuteLocalMock(size_t node_count, size_t local_worker_count,
                 std::function<void(core::JobManager&, size_t)> job_startpoint) {

    // construct a mock mesh network of JobManagers
    std::vector<core::JobManager> jm_mesh
        = core::JobManager::ConstructLocalMesh(node_count, local_worker_count);

    // launch initial thread for each compute node.

    std::vector<std::thread> threads(node_count);
    for (size_t n = 0; n < node_count; n++) {
        threads[n] = std::thread(
            [&jm_mesh, n, job_startpoint] {
                job_startpoint(jm_mesh[n], n);
            });
    }

    // join compute node threads
    for (size_t i = 0; i < node_count; i++) {
        threads[i].join();
    }

    // tear down mock mesh of JobManagers
    // TODO(tb): ???
}

/*!
 * Helper Function to execute tests using mock networks in test suite for many
 * different numbers of node and workers as independent threads in one program.
 */
void ExecuteLocalTests(size_t node_count,
                       std::function<void(Context&)> job_startpoint,
                       const std::string& log_prefix) {

    static const bool debug = false;

    ExecuteLocalMock(
        node_count, 1,
        [job_startpoint, log_prefix](core::JobManager& jm, size_t node_id) {

            Context ctx(jm, 0);
            common::NameThisThread(
                log_prefix + " node " + std::to_string(node_id));

            LOG << "Starting node " << node_id;
            auto overall_timer = ctx.stats().CreateTimer("job::overall", "", true);
            job_startpoint(ctx);
            STOP_TIMER(overall_timer);
            LOG << "Worker " << node_id << " done!";

            jm.flow_manager().GetFlowControlChannel(0).Await();
        });
}

/*!
 * Helper Function to execute tests using mock networks in test suite for many
 * different numbers of node and workers as independent threads in one program.
 */
void ExecuteLocalTests(std::function<void(Context&)> job_startpoint,
                       const std::string& log_prefix) {

    for (size_t nodes = 1; nodes <= 8; ++nodes) {
        ExecuteLocalTests(nodes, job_startpoint, log_prefix);
    }
}

int ExecuteTCP(
    size_t my_rank,
    const std::vector<std::string>& endpoints,
    std::function<void(Context&)> job_startpoint,
    const std::string& log_prefix) {
    static const size_t local_worker_count = 1;

    static const bool debug = true;

    // construct node global objects: net::Manager, data::Manager, etc.

    core::JobManager jobMan(log_prefix);
    jobMan.Connect(my_rank, net::Endpoint::ParseEndpointList(endpoints),
                   local_worker_count);

    // launch initial thread for each of the workers on this node.

    std::vector<std::thread> threads(local_worker_count);
    std::vector<std::atomic<int> > results(local_worker_count);

    for (size_t i = 0; i < local_worker_count; i++) {
        threads[i] = std::thread(
            [&jobMan, &results, &job_startpoint, i, log_prefix] {
                Context ctx(jobMan, i);
                common::NameThisThread(
                    log_prefix + " worker " + std::to_string(i));

                LOG << "Starting job on worker " << ctx.rank();
                auto overall_timer = ctx.stats().CreateTimer("job::overall", "", true);
                // TODO: this cannot be correct, the job needs to know which
                // worker number it is on the node.
                job_startpoint(ctx);
                STOP_TIMER(overall_timer)
                LOG << "Worker " << ctx.rank() << " done!";

                jobMan.flow_manager().GetFlowControlChannel(0).Await();
            });
    }

    // join worker threads
    int global_result = 0;

    for (size_t i = 0; i < local_worker_count; i++) {
        threads[i].join();
    }

    return global_result;
}

// TODO: the following should be renamed to Execute().

/*!
 * Executes the given job startpoint with a context instance.  Startpoints may
 * be called multiple times with concurrent threads and different context
 * instances across different workers.  The c7a configuration is taken from
 * environment variables starting the C7A_.
 *
 * \returns 0 if execution was fine on all threads. Otherwise, the first
 * non-zero return value of any thread is returned.
 */
int ExecuteEnv(
    std::function<void(Context&)> job_startpoint,
    const std::string& log_prefix) {

    char* endptr;

    // parse environment
    const char* c7a_rank = getenv("C7A_RANK");
    const char* c7a_hostlist = getenv("C7A_HOSTLIST");

    if (!c7a_rank || !c7a_hostlist) {
        size_t test_nodes = std::thread::hardware_concurrency();

        const char* c7a_local = getenv("C7A_LOCAL");
        if (c7a_local) {
            // parse envvar only if it exists.
            test_nodes = std::strtoul(c7a_local, &endptr, 10);

            if (!endptr || *endptr != 0 || test_nodes == 0) {
                std::cerr << "environment variable C7A_LOCAL=" << c7a_local
                          << " is not a valid number of local test nodes."
                          << std::endl;
                return -1;
            }
        }

        std::cerr << "c7a: executing locally with " << test_nodes
                  << " test nodes in a local socket network." << std::endl;

        ExecuteLocalTests(test_nodes, job_startpoint, log_prefix);

        return 0;
    }

    size_t my_rank = std::strtoul(c7a_rank, &endptr, 10);

    if (!endptr || *endptr != 0) {
        std::cerr << "environment variable C7A_RANK=" << c7a_rank
                  << " is not a valid number."
                  << std::endl;
        return -1;
    }

    std::vector<std::string> endpoints;

    {
        // first try to split by spaces, then by commas
        std::vector<std::string> hostlist = common::split(c7a_hostlist, ' ');

        if (hostlist.size() == 1) {
            hostlist = common::split(c7a_hostlist, ',');
        }

        for (const std::string& host : hostlist) {
            if (host.find(':') == std::string::npos) {
                std::cerr << "Invalid address \"" << host
                          << "\" in C7A_HOSTLIST. "
                          << "It must contain a port number."
                          << std::endl;
                return -1;
            }
        }

        if (my_rank >= hostlist.size()) {
            std::cerr << "endpoint list (" << hostlist.size() << " entries) "
                      << "does not include my rank (" << my_rank << ")"
                      << std::endl;
            return -1;
        }

        endpoints = hostlist;
    }

    std::cerr << "c7a: executing with rank " << my_rank << " and endpoints";
    for (const std::string& ep : endpoints)
        std::cerr << ' ' << ep;
    std::cerr << std::endl;

    return ExecuteTCP(my_rank, endpoints, job_startpoint, log_prefix);
}

} // namespace api
} // namespace c7a

/******************************************************************************/
