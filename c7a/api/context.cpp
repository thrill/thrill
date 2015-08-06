/*******************************************************************************
 * c7a/api/context.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
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
#include <memory>

namespace c7a {
namespace api {

static inline
std::tuple<int, size_t, size_t, std::vector<std::string> >
ParseArgs(int argc, char* const* argv) {
    //replace with arbitrary complex implementation
    size_t my_rank;
    std::vector<std::string> endpoints;
    c7a::common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    unsigned int rank = 1;
    unsigned int workers_per_host = 1;
    clp.AddUInt('r', "rank", "R", rank,
                "Rank of this host");
    clp.AddUInt('w', "workers", "W", rank,
                "Workers per host");

    std::vector<std::string> addr;
    clp.AddParamStringlist("addresses", addr,
                           "List of all worker addresses.");

    if (!clp.Process(argc, argv)) {
        return std::make_tuple(-1, my_rank, workers_per_host, endpoints);
    }

    for (auto address : addr) {
        if (address.find(":") == std::string::npos) {
            std::cerr << "Invalid address. No Portnumber detectable";
            return std::make_tuple(-1, my_rank, workers_per_host, endpoints);
        }
    }

    if (argc > 3) {
        my_rank = rank;
        endpoints.assign(addr.begin(), addr.end());
    }
    else if (argc == 3) {
        std::cerr << "Wrong number of arguments. Must be 0 or > 1";
        return std::make_tuple(-1, my_rank, workers_per_host, endpoints);
    }
    else {
        my_rank = 0;
        endpoints.push_back("127.0.0.1:1234");
    }
    return std::make_tuple(0, my_rank, workers_per_host, endpoints);
}

//! Executes the given job startpoint with a context instance.
//! Startpoint may be called multiple times with concurrent threads and
//! different context instances.
//!
//! \returns 0 if execution was fine on all threads. Otherwise, the first non-zero return value of any thread is returned.
int Execute(
    int argc, char* const* argv,
    std::function<void(Context&)> job_startpoint,
    const std::string& log_prefix) {

    // true if program time should be taken and printed
    static const bool debug = false;

    size_t my_rank;
    size_t workers_per_host;
    std::vector<std::string> endpoints;
    int global_result = 0;
    std::tie(global_result, my_rank, workers_per_host, endpoints) = ParseArgs(argc, argv);
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

    //connect TCP streams
    net::Manager net_manager;
    net_manager.Initialize(my_rank, net::Endpoint::ParseEndpointList(endpoints));

    //connect data subsystem to network
    data::ChannelMultiplexer cmp(workers_per_host);
    cmp.Connect(&(net_manager.GetDataGroup()));

    //create flow control subsystem
    net::FlowControlChannelManager flow_manager(net_manager.GetFlowGroup(), workers_per_host);

    // launch thread for each of the workers on this host.
    std::vector<std::thread> threads(workers_per_host);
    std::vector<int > results(workers_per_host);

    for (size_t i = 0; i < workers_per_host; i++) {
        threads[i] = std::thread(
            [&net_manager, &cmp, &flow_manager, &job_startpoint, i, log_prefix, workers_per_host] {
                data::Manager data_manager(cmp, i);

                Context ctx(net_manager, flow_manager, data_manager, workers_per_host, i);
                common::NameThisThread(
                    log_prefix + " worker " + std::to_string(i));

                LOG << "Starting job on host " << ctx.host_rank();
                auto overall_timer = ctx.stats().CreateTimer("job::overall", "", true);
                job_startpoint(ctx);
                STOP_TIMER(overall_timer)
                LOG << "Worker " << i << " done!";

                ctx.flow_control_channel().Await();
            });
    }

    // join worker threads

    for (size_t i = 0; i < workers_per_host; i++) {
        threads[i].join();
    }

    return global_result;
}

/*!
 * Starts n hosts with one worker each, all running on this machine.
 * Thus the workers communicate via TCP sockets and do not share a
 * data::Manager or net::FlowControlChannel.
 */
void
ExecuteLocalThreadsTCP(const size_t& workers, const size_t& port_base,
                       std::function<void(Context&)> job_startpoint) {

    std::vector<std::thread> threads(workers);
    std::vector<std::vector<std::string> > strargs(workers);
    std::vector<std::vector<char*> > args(workers);

    for (size_t i = 0; i < workers; i++) {

        // construct command line for independent worker thread

        strargs[i] = { "local_c7a", "-r", std::to_string(i), "-w", "1" };

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
                        job_startpoint, "worker " + std::to_string(i));
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
    std::default_random_engine generator({ std::random_device()() });
    std::uniform_int_distribution<int> distribution(10000, 30000);
    const size_t port_base = distribution(generator);

    for (size_t workers = 1; workers <= 8; workers *= 2) {
        ExecuteLocalThreadsTCP(workers, port_base, job_startpoint);
    }
}

/*!
 * Starts n hosts with multiple workers each, all running on this machine.
 * The hosts communicate via Sockets created by the socketpair call and do
 * not share a data::Manager or net::FlowControlChannel. The workers withing
 * the same host do share these components though.
 */
void
ExecuteLocalMock(size_t host_count, size_t workers_per_host,
                 std::function<void(api::Context&)> job_startpoint) {
    static const bool debug = false;
    //connect TCP streams
    std::vector<net::Manager> net_managers = net::Manager::ConstructLocalMesh(host_count);
    assert(net_managers.size() == host_count);

    //cannot be constructed inside loop because we pass only references down
    //thus the objects must live longer than the loop.
    std::vector<data::ChannelMultiplexer> channel_multiplexers;
    std::vector<net::FlowControlChannelManager> flow_managers;
    channel_multiplexers.reserve(host_count);
    flow_managers.reserve(host_count);


    for (size_t host = 0; host < host_count; host++) {
        //connect data subsystem to network
        channel_multiplexers.emplace_back(workers_per_host);
        //TOOD(ts) fix this ugly pointer workaround ??
        channel_multiplexers[host].Connect(&(net_managers[host].GetDataGroup()));

        //create flow control subsystem
        auto & group = net_managers[host].GetFlowGroup();
        flow_managers.emplace_back(group, workers_per_host);
    }

    // launch thread for each of the workers on this host.
    std::vector<std::thread> threads(host_count * workers_per_host);

    for (size_t host = 0; host < host_count; host++) {
        std::string log_prefix = "host " + std::to_string(host);
        for (size_t i = 0; i < workers_per_host; i++) {
            threads[host * workers_per_host + i] = std::thread(
                [&net_managers, &channel_multiplexers, &flow_managers, &job_startpoint, host, i, log_prefix, workers_per_host ] {
                    data::Manager data_manager(channel_multiplexers[host], i);
                    Context ctx(net_managers[host], flow_managers[host], data_manager, workers_per_host, i);
                    common::NameThisThread(
                        log_prefix + " worker " + std::to_string(i));

                    LOG << "Starting job on host " << ctx.host_rank();
                    auto overall_timer = ctx.stats().CreateTimer("job::overall", "", true);
                    job_startpoint(ctx);
                    STOP_TIMER(overall_timer)
                    LOG << "Worker " << i << " done!";
                    ctx.flow_control_channel().Await();
                });
        }
    }

    // join worker threads

    for (size_t i = 0; i < host_count * workers_per_host; i++) {
        threads[i].join();
    }
}

/*!
 * Helper Function to execute tests using mock networks in test suite for many
 * different numbers of host and workers as independent threads in one program.
 */
void ExecuteLocalTests(std::function<void(Context&)> job_startpoint) {
    int num_hosts[] = { 1, 2, 5, 8};
    int num_workers[] = {1};//, 2, 3};

    for (auto& hosts : num_hosts) {
        for (auto& workers : num_workers) {
            ExecuteLocalMock(hosts, workers, job_startpoint);
        }
    }
}

void ExecuteSameThread(std::function<void(Context&)> job_startpoint) {
    net::Manager net_manager;
    net_manager.Initialize(0, net::Endpoint::ParseEndpointList("127.0.0.1:12345"));

    size_t workers_per_host = 1;
    size_t my_host_rank = 1;

    //connect data subsystem to network
    data::ChannelMultiplexer multiplexer(workers_per_host);
    multiplexer.Connect(&net_manager.GetDataGroup());
    data::Manager data_manager(multiplexer, my_host_rank);

    //create flow control subsystem
    net::FlowControlChannelManager flow_manager(net_manager.GetFlowGroup(), 1);

    Context ctx(net_manager, flow_manager, data_manager, workers_per_host, my_host_rank);
    common::NameThisThread("worker " + std::to_string(my_host_rank));

    job_startpoint(ctx);
}


int ExecuteTCP(
    size_t my_rank,
    const std::vector<std::string>& endpoints,
    std::function<void(Context&)> job_startpoint,
    const std::string& log_prefix) {

    //TODO pull this out of ENV
    const size_t workers_per_host = 1;

    static const bool debug = false;

    net::Manager net_manager;
    net_manager.Initialize(my_rank, net::Endpoint::ParseEndpointList(endpoints));

    data::ChannelMultiplexer cmp(workers_per_host);
    cmp.Connect(&(net_manager.GetDataGroup()));
    net::FlowControlChannelManager flow_manager(net_manager.GetFlowGroup(), workers_per_host);

    std::vector<std::thread> threads(workers_per_host);

    for (size_t i = 0; i < workers_per_host; i++) {
        threads[i] = std::thread(
            [&net_manager, &cmp, &flow_manager, &job_startpoint, i, log_prefix, workers_per_host] {
                data::Manager data_manager(cmp, i);
                Context ctx(net_manager, flow_manager, data_manager, workers_per_host, i);
                common::NameThisThread(
                    log_prefix + " worker " + std::to_string(i));

                LOG << "Starting job on worker " << ctx.my_rank() << " (" << ctx.host_rank() << "/" << ctx.worker_id() << ")";
                auto overall_timer = ctx.stats().CreateTimer("job::overall", "", true);
                job_startpoint(ctx);
                STOP_TIMER(overall_timer)
                LOG << "Worker " << ctx.my_rank() << " done!";

                ctx.flow_control_channel().Await();
            });
    }

    // join worker threads
    int global_result = 0;

    for (size_t i = 0; i < workers_per_host; i++) {
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
    const std::string& /*log_prefix*/) {

    char* endptr;

    // parse environment
    const char* c7a_rank = getenv("C7A_RANK");
    const char* c7a_hostlist = getenv("C7A_HOSTLIST");

    if (!c7a_rank || !c7a_hostlist) {
        size_t test_hosts = std::thread::hardware_concurrency();

        const char* c7a_local = getenv("C7A_LOCAL");
        if (c7a_local) {
            // parse envvar only if it exists.
            test_hosts = std::strtoul(c7a_local, &endptr, 10);

            if (!endptr || *endptr != 0 || test_hosts == 0) {
                std::cerr << "environment variable C7A_LOCAL=" << c7a_local
                          << " is not a valid number of local test hosts."
                          << std::endl;
                return -1;
            }
        }

        std::cerr << "c7a: executing locally with " << test_hosts
                  << " test hosts in a local socket network." << std::endl;

        const size_t workers_per_host = 1;
        ExecuteLocalMock(test_hosts, workers_per_host, job_startpoint);

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

    return ExecuteTCP(my_rank, endpoints, job_startpoint, "");
}

} // namespace api
} // namespace c7a

/******************************************************************************/
