/*******************************************************************************
 * thrill/api/context.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/api/context.hpp>

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats.hpp>
#include <thrill/net/tcp/construct.hpp>

#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

namespace thrill {
namespace api {

//! Construct a number of mock hosts running in this process.
std::vector<std::unique_ptr<HostContext> >
HostContext::ConstructLocalMock(size_t host_count, size_t workers_per_host) {
    static const size_t kGroupCount = net::Manager::kGroupCount;

    // construct three full mesh connection cliques, deliver net::tcp::Groups.
    std::array<std::vector<std::unique_ptr<net::tcp::Group> >, kGroupCount> group;

    for (size_t g = 0; g < kGroupCount; ++g) {
        group[g] = net::tcp::Group::ConstructLocalMesh(host_count);
    }

    // construct host context
    std::vector<std::unique_ptr<HostContext> > host_context;

    for (size_t h = 0; h < host_count; h++) {
        std::array<net::GroupPtr, kGroupCount> host_group = {
            { std::move(group[0][h]),
              std::move(group[1][h]),
              std::move(group[2][h]) }
        };

        host_context.emplace_back(
            std::make_unique<HostContext>(
                std::move(host_group), workers_per_host));
    }

    return host_context;
}

/*!
 * Starts n hosts with multiple workers each, all running on this machine.
 * The hosts communicate via Sockets created by the socketpair call and do
 * not share a data::Multiplexer or net::FlowControlChannel. The workers withing
 * the same host do share these components though.
 */
void
RunLocalMock(size_t host_count, size_t workers_per_host,
             const std::function<void(Context&)>& job_startpoint) {
    static const bool debug = false;

    // construct a mock network of hosts
    std::vector<std::unique_ptr<HostContext> > host_contexts =
        HostContext::ConstructLocalMock(host_count, workers_per_host);

    // launch thread for each of the workers on this host.
    std::vector<std::thread> threads(host_count * workers_per_host);

    for (size_t host = 0; host < host_count; host++) {
        mem::by_string log_prefix = "host " + mem::to_string(host);
        for (size_t worker = 0; worker < workers_per_host; worker++) {
            threads[host * workers_per_host + worker] = std::thread(
                [&host_contexts, &job_startpoint, host, worker, log_prefix] {
                    Context ctx(*host_contexts[host], worker);
                    common::NameThisThread(
                        log_prefix + " worker " + mem::to_string(worker));

                    LOG << "Starting job on host " << ctx.host_rank();
                    auto overall_timer = ctx.stats().CreateTimer("job::overall", "", true);
                    job_startpoint(ctx);
                    STOP_TIMER(overall_timer)
                    LOG << "Worker " << worker << " done!";
                    ctx.Barrier();
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
void RunLocalTests(const std::function<void(Context&)>& job_startpoint) {
    int num_hosts[] = { 1, 2, 5, 8 };
    int num_workers[] = { 1 };//, 2, 3};

    for (auto& hosts : num_hosts) {
        for (auto& workers : num_workers) {
            RunLocalMock(hosts, workers, job_startpoint);
        }
    }
}

HostContext::HostContext(size_t my_host_rank,
                         const std::vector<std::string>& endpoints,
                         size_t workers_per_host)
    : workers_per_host_(workers_per_host),
      net_manager_(net::tcp::Construct(my_host_rank,
                                       endpoints, net::Manager::kGroupCount)),
      flow_manager_(net_manager_.GetFlowGroup(), workers_per_host),
      data_multiplexer_(mem_manager_,
                        block_pool_, workers_per_host,
                        net_manager_.GetDataGroup())
{ }

void RunSameThread(const std::function<void(Context&)>& job_startpoint) {

    size_t my_host_rank = 0;
    size_t workers_per_host = 1;

    HostContext host_context(
        my_host_rank, { "127.0.0.1:12345" }, workers_per_host);

    Context ctx(host_context, 0);
    common::NameThisThread("worker " + mem::to_string(my_host_rank));

    job_startpoint(ctx);
}

int RunDistributedTCP(
    size_t my_host_rank,
    const std::vector<std::string>& endpoints,
    const std::function<void(Context&)>& job_startpoint,
    const mem::by_string& log_prefix) {

    // TODO pull this out of ENV
    const size_t workers_per_host = 1;

    static const bool debug = false;

    HostContext host_context(my_host_rank, endpoints, workers_per_host);

    std::vector<std::thread> threads(workers_per_host);

    for (size_t i = 0; i < workers_per_host; i++) {
        threads[i] = std::thread(
            [&host_context, &job_startpoint, i, log_prefix] {
                Context ctx(host_context, i);
                common::NameThisThread(
                    log_prefix + " worker " + mem::to_string(i));

                LOG << "Starting job on worker " << ctx.my_rank() << " (" << ctx << ")";
                auto overall_timer = ctx.stats().CreateTimer("job::overall", "", true);
                job_startpoint(ctx);
                STOP_TIMER(overall_timer)
                LOG << "Worker " << ctx.my_rank() << " done!";

                ctx.Barrier();
            });
    }

    // join worker threads
    int global_result = 0;

    for (size_t i = 0; i < workers_per_host; i++) {
        threads[i].join();
    }

    return global_result;
}

/*!
 * Runs the given job startpoint with a context instance.  Startpoints may be
 * called multiple times with concurrent threads and different context instances
 * across different workers.  The Thrill configuration is taken from environment
 * variables starting the THRILL_.
 *
 * \returns 0 if execution was fine on all threads. Otherwise, the first
 * non-zero return value of any thread is returned.
 */
int Run(
    const std::function<void(Context&)>& job_startpoint,
    const std::string& /*log_prefix*/) {

    char* endptr;

    // parse environment
    const char* env_rank = getenv("THRILL_RANK");
    const char* env_hostlist = getenv("THRILL_HOSTLIST");

    if (!env_rank || !env_hostlist) {
        size_t test_hosts = std::thread::hardware_concurrency();

        const char* env_local = getenv("THRILL_LOCAL");
        if (env_local) {
            // parse envvar only if it exists.
            test_hosts = std::strtoul(env_local, &endptr, 10);

            if (!endptr || *endptr != 0 || test_hosts == 0) {
                std::cerr << "environment variable THRILL_LOCAL=" << env_local
                          << " is not a valid number of local test hosts."
                          << std::endl;
                return -1;
            }
        }

        std::cerr << "Thrill: executing locally with " << test_hosts
                  << " test hosts in a local socket network." << std::endl;

        const size_t workers_per_host = 1;
        RunLocalMock(test_hosts, workers_per_host, job_startpoint);

        return 0;
    }

    size_t my_host_rank = std::strtoul(env_rank, &endptr, 10);

    if (!endptr || *endptr != 0) {
        std::cerr << "environment variable THRILL_RANK=" << env_rank
                  << " is not a valid number."
                  << std::endl;
        return -1;
    }

    std::vector<std::string> endpoints;

    {
        // first try to split by spaces, then by commas
        std::vector<std::string> hostlist = common::split(env_hostlist, ' ');

        if (hostlist.size() == 1) {
            hostlist = common::split(env_hostlist, ',');
        }

        for (const std::string& host : hostlist) {
            // skip empty splits
            if (host.size() == 0) continue;

            if (host.find(':') == std::string::npos) {
                std::cerr << "Invalid address \"" << host << "\""
                          << "in THRILL_HOSTLIST. It must contain a port number."
                          << std::endl;
                return -1;
            }

            endpoints.push_back(host);
        }

        if (my_host_rank >= endpoints.size()) {
            std::cerr << "endpoint list (" << hostlist.size() << " entries) "
                      << "does not include my host_rank (" << my_host_rank << ")"
                      << std::endl;
            return -1;
        }
    }

    std::cerr << "Thrill: executing with host_rank " << my_host_rank << " and endpoints";
    for (const std::string& ep : endpoints)
        std::cerr << ' ' << ep;
    std::cerr << std::endl;

    return RunDistributedTCP(my_host_rank, endpoints, job_startpoint, "");
}

} // namespace api
} // namespace thrill

/******************************************************************************/
