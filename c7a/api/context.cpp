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
    unsigned int workers_per_node = 1;
    clp.AddUInt('r', "rank", "R", rank,
                "Rank of this computing node");
    clp.AddUInt('w', "workers", "W", rank,
                "Workers per computing node");

    std::vector<std::string> addr;
    clp.AddParamStringlist("addresses", addr,
                           "List of all worker addresses.");

    if (!clp.Process(argc, argv)) {
        return std::make_tuple(-1, my_rank, workers_per_node, endpoints);
    }

    for (auto address : addr) {
        if (address.find(":") == std::string::npos) {
            std::cerr << "Invalid address. No Portnumber detectable";
            return std::make_tuple(-1, my_rank, workers_per_node, endpoints);
        }
    }

    if (argc > 3) {
        my_rank = rank;
        endpoints.assign(addr.begin(), addr.end());
    }
    else if (argc == 3) {
        std::cerr << "Wrong number of arguments. Must be 0 or > 1";
        return std::make_tuple(-1, my_rank, workers_per_node, endpoints);
    }
    else {
        my_rank = 0;
        endpoints.push_back("127.0.0.1:1234");
    }
    return std::make_tuple(0, my_rank, workers_per_node, endpoints);
}

//! Executes the given job startpoint with a context instance.
//! Startpoint may be called multiple times with concurrent threads and
//! different context instances.
//!
//! \returns 0 if execution was fine on all threads. Otherwise, the first non-zero return value of any thread is returned.
int Execute(
    int argc, char* const* argv,
    std::function<int(Context&)> job_startpoint,
    const std::string& log_prefix) {

    // true if program time should be taken and printed
    static const bool debug = false;

    size_t my_rank;
    size_t workers_per_node;
    std::vector<std::string> endpoints;
    int global_result = 0;
    std::tie(global_result, my_rank, workers_per_node, endpoints) = ParseArgs(argc, argv);
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
    data::ChannelMultiplexer cmp(workers_per_node);
    cmp.Connect(&(net_manager.GetDataGroup()));

    //create flow control subsystem
    net::FlowControlChannelManager flow_manager(net_manager.GetFlowGroup(), workers_per_node);

    // launch thread for each of the workers on this node.
    std::vector<std::thread> threads(workers_per_node);
    std::vector<int > results(workers_per_node);

    for (size_t i = 0; i < workers_per_node; i++) {
        threads[i] = std::thread(
            [&net_manager, &cmp, &flow_manager, &results, &job_startpoint, i, log_prefix, workers_per_node] {
                data::Manager data_manager(cmp, i);

                Context ctx(net_manager, flow_manager, data_manager, workers_per_node, i);
                common::NameThisThread(
                    log_prefix + " worker " + std::to_string(i));

                LOG << "Starting job on node " << ctx.computing_node_id();
                auto overall_timer = ctx.stats().CreateTimer("job::overall", "", true);
                int job_result = job_startpoint(ctx);
                STOP_TIMER(overall_timer)
                LOG << "Worker " << i << " done!";

                results[i] = job_result;
                ctx.flow_control_channel().Await();
            });
    }

    // join worker threads

    for (size_t i = 0; i < workers_per_node; i++) {
        threads[i].join();
        if (results[i] != 0 && global_result == 0)
            global_result = results[i];
    }

    return global_result;
}

/*!
 * Starts n computing nodes with one worker each, all running on this machine.
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

        std::function<int(Context&)> intReturningFunction =
            [job_startpoint](Context& ctx) {
                job_startpoint(ctx);
                return 1;
            };

        threads[i] = std::thread(
            [=]() {
                Execute(strargs[i].size(), args[i].data(),
                        intReturningFunction, "worker " + std::to_string(i));
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
 * Starts n computing nodes with multiple workers each, all running on this machine.
 * The nodes communicate via Sockets created by the socketpair call and do
 * not share a data::Manager or net::FlowControlChannel. The workers withing
 * the same node do share these components though.
 */
void
ExecuteLocalMock(size_t node_count, size_t workers_per_node,
                 std::function<void(api::Context&)> job_startpoint) {
    static const bool debug = false;
    //connect TCP streams
    std::vector<net::Manager> net_managers = net::Manager::ConstructLocalMesh(node_count);
    assert(net_managers.size() == node_count);

    //cannot be constructed inside loop because we pass only references down
    //thus the objects must live longer than the loop.
    std::vector<data::ChannelMultiplexer> channel_multiplexers;
    std::vector<net::FlowControlChannelManager> flow_managers;
    channel_multiplexers.reserve(node_count);
    flow_managers.reserve(node_count);


    for (size_t node = 0; node < node_count; node++) {
        //connect data subsystem to network
        channel_multiplexers.emplace_back(workers_per_node);
        //TOOD(ts) fix this ugly pointer workaround ??
        channel_multiplexers[node].Connect(&(net_managers[node].GetDataGroup()));

        //create flow control subsystem
        auto & group = net_managers[node].GetFlowGroup();
        flow_managers.emplace_back(group, workers_per_node);
    }

    // launch thread for each of the workers on this node.
    std::vector<std::thread> threads(node_count * workers_per_node);

    for (size_t node = 0; node < node_count; node++) {
        std::string log_prefix = "node " + std::to_string(node);
        for (size_t i = 0; i < workers_per_node; i++) {
            threads[node * workers_per_node + i] = std::thread(
                [&net_managers, &channel_multiplexers, &flow_managers, &job_startpoint, node, i, log_prefix, workers_per_node ] {
                    data::Manager data_manager(channel_multiplexers[node], i);
                    Context ctx(net_managers[node], flow_managers[node], data_manager, workers_per_node, i);
                    common::NameThisThread(
                        log_prefix + " worker " + std::to_string(i));

                    LOG << "Starting job on node " << ctx.computing_node_id();
                    auto overall_timer = ctx.stats().CreateTimer("job::overall", "", true);
                    job_startpoint(ctx);
                    STOP_TIMER(overall_timer)
                    LOG << "Worker " << i << " done!";
                    ctx.flow_control_channel().Await();
                });
        }
    }

    // join worker threads

    for (size_t i = 0; i < node_count * workers_per_node; i++) {
        threads[i].join();
    }
}

/*!
 * Helper Function to execute tests using mock networks in test suite for many
 * different numbers of node and workers as independent threads in one program.
 */
void ExecuteLocalTests(std::function<void(Context&)> job_startpoint) {
    int num_nodes[] = { 1, 2, 5, 8};
    int num_workers[] = {1};//, 2, 3};

    for (auto& nodes : num_nodes) {
        for (auto& workers : num_workers) {
            ExecuteLocalMock(nodes, workers, job_startpoint);
        }
    }
}

void ExecuteSameThread(std::function<void(Context&)> job_startpoint) {
    net::Manager net_manager;
    net_manager.Initialize(0, net::Endpoint::ParseEndpointList("127.0.0.1:12345"));

    size_t workers_per_node = 1;
    size_t my_node_id = 1;

    //connect data subsystem to network
    data::ChannelMultiplexer multiplexer(workers_per_node);
    multiplexer.Connect(&net_manager.GetDataGroup());
    data::Manager data_manager(multiplexer, my_node_id);

    //create flow control subsystem
    net::FlowControlChannelManager flow_manager(net_manager.GetFlowGroup(), 1);

    Context ctx(net_manager, flow_manager, data_manager, workers_per_node, my_node_id);
    common::NameThisThread("worker " + std::to_string(my_node_id));

    job_startpoint(ctx);
}

} // namespace api
} // namespace c7a

/******************************************************************************/
