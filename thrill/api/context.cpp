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
#include <thrill/common/math.hpp>
#include <thrill/common/stat_logger.hpp>
#include <thrill/common/stats.hpp>

// mock net backend is always available -tb :)
#include <thrill/net/mock/group.hpp>

//#define THRILL_HAVE_NET_MPI 1

#if THRILL_HAVE_NET_TCP
#include <thrill/net/tcp/construct.hpp>
#endif

#if THRILL_HAVE_NET_MPI
#include <thrill/net/mpi/group.hpp>
#endif

#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

namespace thrill {
namespace api {

/******************************************************************************/
// Generic Network Construction

//! Generic network constructor for net backends supporting loopback tests.
template <typename NetGroup>
static inline
std::vector<std::unique_ptr<HostContext> >
ConstructLoopbackHostContexts(size_t host_count, size_t workers_per_host) {
    static const size_t kGroupCount = net::Manager::kGroupCount;

    // construct three full mesh loopback cliques, deliver net::Groups.
    std::array<std::vector<std::unique_ptr<NetGroup> >, kGroupCount> group;

    for (size_t g = 0; g < kGroupCount; ++g) {
        group[g] = NetGroup::ConstructLoopbackMesh(host_count);
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

//! Generic runner for backends supporting loopback tests.
template <typename NetGroup>
static inline void
RunLoopbackThreads(size_t host_count, size_t workers_per_host,
                   const std::function<void(Context&)>& job_startpoint) {
    static const bool debug = false;

    // construct a mock network of hosts
    std::vector<std::unique_ptr<HostContext> > host_contexts =
        ConstructLoopbackHostContexts<NetGroup>(host_count, workers_per_host);

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
                    try {
                        job_startpoint(ctx);
                    }
                    catch (std::exception& e) {
                        LOG1 << "Worker " << worker
                             << " threw " << typeid(e).name();
                        LOG1 << "  what(): " << e.what();
                        throw;
                    }
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

/******************************************************************************/
// Runners using Mock Net Backend

static inline void
RunLoopbackMock(size_t host_count, size_t workers_per_host,
             const std::function<void(Context&)>& job_startpoint) {

    return RunLoopbackThreads<net::mock::Group>(
        host_count, workers_per_host, job_startpoint);
}

/******************************************************************************/
// Runners using TCP Net Backend

#if THRILL_HAVE_NET_TCP
static inline void
RunLoopbackTCP(size_t host_count, size_t workers_per_host,
             const std::function<void(Context&)>& job_startpoint) {

    return RunLoopbackThreads<net::tcp::Group>(
        host_count, workers_per_host, job_startpoint);
}
#endif

/******************************************************************************/
// Constructions using TestGroup (either mock or tcp-loopback) for local testing

#if defined(_MSC_VER)
using TestGroup = net::mock::Group;
#else
using TestGroup = net::tcp::Group;
#endif

void
RunLocalMock(size_t host_count, size_t workers_per_host,
             const std::function<void(Context&)>& job_startpoint) {

    return RunLoopbackThreads<TestGroup>(
        host_count, workers_per_host, job_startpoint);
}

std::vector<std::unique_ptr<HostContext> >
HostContext::ConstructLoopback(size_t host_count, size_t workers_per_host) {

    return ConstructLoopbackHostContexts<TestGroup>(
        host_count, workers_per_host);
}

void RunLocalTests(const std::function<void(Context&)>& job_startpoint) {
    size_t num_hosts[] = { 1, 2, 5, 8 };
    size_t num_workers[] = { 1, 3 };

    for (size_t& host_count : num_hosts) {
        for (size_t& workers_per_host : num_workers) {
            return RunLoopbackThreads<TestGroup>(
                host_count, workers_per_host, job_startpoint);
        }
    }
}

void RunLocalSameThread(const std::function<void(Context&)>& job_startpoint) {

    size_t my_host_rank = 0;
    size_t workers_per_host = 1;
    size_t host_count = 1;
    static const size_t kGroupCount = net::Manager::kGroupCount;

    // construct three full mesh connection cliques, deliver net::tcp::Groups.
    std::array<std::vector<std::unique_ptr<TestGroup> >, kGroupCount> group;

    for (size_t g = 0; g < kGroupCount; ++g) {
        group[g] = TestGroup::ConstructLoopbackMesh(host_count);
    }

    std::array<net::GroupPtr, kGroupCount> host_group = {
        { std::move(group[0][0]),
          std::move(group[1][0]),
          std::move(group[2][0]) }
    };

    HostContext host_context(std::move(host_group), workers_per_host);

    Context ctx(host_context, 0);
    common::NameThisThread("worker " + mem::to_string(my_host_rank));

    job_startpoint(ctx);
}

/******************************************************************************/
// Run() Variants for Different Net Backends

//! Run() implementation which uses a loopback net backend.
template <typename NetGroup>
static inline
int RunBackendLoopback(const std::function<void(Context&)>& job_startpoint) {

    char* endptr;

    // determine number of loopback hosts

    size_t host_count = std::thread::hardware_concurrency();

    const char* env_local = getenv("THRILL_LOCAL");
    if (env_local) {
        // parse envvar only if it exists.
        host_count = std::strtoul(env_local, &endptr, 10);

        if (!endptr || *endptr != 0 || host_count == 0) {
            std::cerr << "Thrill: environment variable"
                      << " THRILL_LOCAL=" << env_local
                      << " is not a valid number of local loopback hosts."
                      << std::endl;
            return -1;
        }
    }

    const char* env_workers_per_host = getenv("THRILL_WORKERS_PER_HOST");

    // determine number of threads per loopback host

    size_t workers_per_host = 1;
    if (env_workers_per_host) {
        workers_per_host = std::strtoul(env_workers_per_host, &endptr, 10);
        if (!endptr || *endptr != 0 || workers_per_host == 0) {
            std::cerr << "Thrill: environment variable"
                      << " THRILL_WORKERS_PER_HOST=" << env_workers_per_host
                      << " is not a valid number of workers per host."
                      << std::endl;
            return -1;
        }
    }
    else {
        if (!env_local && !env_workers_per_host) {
            // distribute two threads per worker.
            workers_per_host = 2;
            host_count /= 2;
        }
    }

    std::cerr << "Thrill: executing locally with " << host_count
              << " test hosts and " << workers_per_host << " workers per host"
              << " in a local mock network." << std::endl;

    RunLoopbackThreads<NetGroup>(
        host_count, workers_per_host, job_startpoint);

    return 0;
}

#if THRILL_HAVE_NET_TCP
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
#endif

#if THRILL_HAVE_NET_TCP
static inline
int RunBackendTcp(const std::function<void(Context&)>& job_startpoint) {

    char* endptr;

    // parse environment
    const char* env_rank = getenv("THRILL_RANK");
    const char* env_hostlist = getenv("THRILL_HOSTLIST");
    const char* env_workers_per_host = getenv("THRILL_WORKERS_PER_HOST");

    size_t my_host_rank = 0;

    if (env_rank) {
        my_host_rank = std::strtoul(env_rank, &endptr, 10);

        if (!endptr || *endptr != 0) {
            std::cerr << "Thrill: environment variable THRILL_RANK=" << env_rank
                      << " is not a valid number."
                      << std::endl;
            return -1;
        }
    }
    else {
        std::cerr << "Thrill: environment variable THRILL_RANK"
                  << " is required for tcp network backend.";
        return -1;
    }

    std::vector<std::string> hostlist;

    if (env_hostlist) {
        // first try to split by spaces, then by commas
        std::vector<std::string> list = common::split(env_hostlist, ' ');

        if (list.size() == 1) {
            list = common::split(env_hostlist, ',');
        }

        for (const std::string& host : list) {
            // skip empty splits
            if (host.size() == 0) continue;

            if (host.find(':') == std::string::npos) {
                std::cerr << "Thrill: invalid address \"" << host << "\""
                          << "in THRILL_HOSTLIST. It must contain a port number."
                          << std::endl;
                return -1;
            }

            hostlist.push_back(host);
        }

        if (my_host_rank >= hostlist.size()) {
            std::cerr << "Thrill: endpoint list (" << list.size() << " entries) "
                      << "does not include my host_rank (" << my_host_rank << ")"
                      << std::endl;
            return -1;
        }
    }
    else {
        std::cerr << "Thrill: environment variable THRILL_HOSTLIST"
                  << " is required for tcp network backend.";
        return -1;
    }

    size_t workers_per_host = 1;

    if (env_workers_per_host) {
        workers_per_host = std::strtoul(env_workers_per_host, &endptr, 10);
        if (!endptr || *endptr != 0 || workers_per_host == 0) {
            std::cerr << "Thrill: environment variable"
                      << " THRILL_WORKERS_PER_HOST=" << env_workers_per_host
                      << " is not a valid number of workers per host."
                      << std::endl;
            return -1;
        }
    }
    else {
        // TODO: someday, set workers_per_host = std::thread::hardware_concurrency().
    }

    // okay configuration is good.

    std::cerr << "Thrill: executing with host_rank " << my_host_rank << " and endpoints";
    for (const std::string& ep : hostlist)
        std::cerr << ' ' << ep;
    std::cerr << std::endl;

    STAT_NO_RANK << "event" << "RunDistributedTCP"
                 << "my_host_rank" << my_host_rank
                 << "workers_per_host" << workers_per_host;

    HostContext host_context(my_host_rank, hostlist, workers_per_host);

    std::vector<std::thread> threads(workers_per_host);

    for (size_t i = 0; i < workers_per_host; i++) {
        threads[i] = std::thread(
            [&host_context, &job_startpoint, i] {
                Context ctx(host_context, i);
                common::NameThisThread(" worker " + mem::to_string(i));

                STAT(ctx) << "event" << "jobStart";
                auto overall_timer = ctx.stats().CreateTimer("job::overall", "", true);
                job_startpoint(ctx);
                STOP_TIMER(overall_timer)
                if (overall_timer)
                    STAT(ctx) << "event" << "jobDone"
                              << "time" << overall_timer->Milliseconds();

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
#endif

static inline int
RunNotSupported(const char* env_net) {
    std::cerr << "Thrill: network backend " << env_net
              << " is not supported by this binary.";
    return -1;
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
int Run(const std::function<void(Context&)>& job_startpoint) {

    // parse environment.
    const char* env_net = getenv("THRILL_NET");

    // if no backend configured: automatically select one.
    if (!env_net) {
#if defined(_MSC_VER)
        env_net = "mock";
#else
        const char* env_rank = getenv("THRILL_RANK");
        const char* env_hostlist = getenv("THRILL_HOSTLIST");

        if (env_rank || env_hostlist)
            env_net = "tcp";
        else
            env_net = "local";
#endif
    }

    // run with selected backend
    if (strcmp(env_net, "mock") == 0) {
        // mock network backend
        return RunBackendLoopback<net::mock::Group>(job_startpoint);
    }

    if (strcmp(env_net, "local") == 0) {
#if THRILL_HAVE_NET_TCP
        // tcp loopback network backend
        return RunBackendLoopback<net::tcp::Group>(job_startpoint);
#else
        return RunNotSupported(env_net);
#endif
    }

    if (strcmp(env_net, "tcp") == 0) {
#if THRILL_HAVE_NET_TCP
        // real tcp network backend
        return RunBackendTcp(job_startpoint);
#else
        return RunNotSupported(env_net);
#endif
    }

    std::cerr << "Thrill: network backend " << env_net << " is unknown.";
    return -1;
}

} // namespace api
} // namespace thrill

/******************************************************************************/
