/*******************************************************************************
 * thrill/api/context.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/context.hpp>

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <thrill/common/system_exception.hpp>

// mock net backend is always available -tb :)
#include <thrill/net/mock/group.hpp>

#if THRILL_HAVE_NET_TCP
#include <thrill/net/tcp/construct.hpp>
#endif

#if THRILL_HAVE_NET_MPI
#include <thrill/net/mpi/group.hpp>
#endif

#if __linux__

// linux-specific process control
#include <sys/prctl.h>

#endif

#if defined(_MSC_VER)

// for detecting amount of physical memory
#include <windows.h>

#endif

#include <csignal>
#include <memory>
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
ConstructLoopbackHostContexts(
    const MemoryConfig& mem_config, size_t num_hosts, size_t workers_per_host) {

    static const size_t kGroupCount = net::Manager::kGroupCount;

    // construct three full mesh loopback cliques, deliver net::Groups.
    std::array<std::vector<std::unique_ptr<NetGroup> >, kGroupCount> group;

    for (size_t g = 0; g < kGroupCount; ++g) {
        group[g] = NetGroup::ConstructLoopbackMesh(num_hosts);
    }

    // construct host context
    std::vector<std::unique_ptr<HostContext> > host_context;

    for (size_t h = 0; h < num_hosts; h++) {
        std::array<net::GroupPtr, kGroupCount> host_group = {
            { std::move(group[0][h]), std::move(group[1][h]) }
        };

        host_context.emplace_back(
            std::make_unique<HostContext>(
                mem_config, std::move(host_group), workers_per_host));
    }

    return host_context;
}

//! Generic runner for backends supporting loopback tests.
template <typename NetGroup>
static inline void
RunLoopbackThreads(
    const MemoryConfig& mem_config,
    size_t num_hosts, size_t workers_per_host,
    const std::function<void(Context&)>& job_startpoint) {

    MemoryConfig host_mem_config = mem_config.divide(num_hosts);
    mem_config.print(workers_per_host);

    // construct a mock network of hosts
    std::vector<std::unique_ptr<HostContext> > host_contexts =
        ConstructLoopbackHostContexts<NetGroup>(
            host_mem_config, num_hosts, workers_per_host);

    // launch thread for each of the workers on this host.
    std::vector<std::thread> threads(num_hosts * workers_per_host);

    for (size_t host = 0; host < num_hosts; ++host) {
        mem::by_string log_prefix = "host " + mem::to_string(host);
        for (size_t worker = 0; worker < workers_per_host; ++worker) {
            threads[host * workers_per_host + worker] = std::thread(
                [&host_contexts, &job_startpoint, host, worker, log_prefix] {
                    Context ctx(*host_contexts[host], worker);
                    common::NameThisThread(
                        log_prefix + " worker " + mem::to_string(worker));

                    ctx.Launch(job_startpoint);
                });
        }
    }

    // join worker threads
    for (size_t i = 0; i < num_hosts * workers_per_host; i++) {
        threads[i].join();
    }
}

/******************************************************************************/
// Constructions using TestGroup (either mock or tcp-loopback) for local testing

#if defined(_MSC_VER)
using TestGroup = net::mock::Group;
#else
using TestGroup = net::tcp::Group;
#endif

void RunLocalMock(size_t num_hosts, size_t workers_per_host,
                  const std::function<void(Context&)>& job_startpoint) {

    MemoryConfig mem_config;
    mem_config.setup_test();

    return RunLoopbackThreads<TestGroup>(
        mem_config, num_hosts, workers_per_host, job_startpoint);
}

std::vector<std::unique_ptr<HostContext> >
HostContext::ConstructLoopback(size_t num_hosts, size_t workers_per_host) {

    MemoryConfig mem_config;
    mem_config.setup_test();

    return ConstructLoopbackHostContexts<TestGroup>(
        mem_config, num_hosts, workers_per_host);
}

void RunLocalTests(const std::function<void(Context&)>& job_startpoint) {

    // discard json log
    setenv("THRILL_LOG", "", /* overwrite */ 1);

    static const size_t num_hosts_list[] = { 1, 2, 5, 8 };
    static const size_t num_workers_list[] = { 1, 3 };

    for (const size_t& num_hosts : num_hosts_list) {
        for (const size_t& workers_per_host : num_workers_list) {
            RunLocalMock(num_hosts, workers_per_host, job_startpoint);
        }
    }
}

void RunLocalSameThread(const std::function<void(Context&)>& job_startpoint) {

    size_t my_host_rank = 0;
    size_t workers_per_host = 1;
    size_t num_hosts = 1;
    static const size_t kGroupCount = net::Manager::kGroupCount;

    MemoryConfig mem_config;
    mem_config.setup_test();
    mem_config.print(workers_per_host);

    // construct three full mesh connection cliques, deliver net::tcp::Groups.
    std::array<std::vector<std::unique_ptr<TestGroup> >, kGroupCount> group;

    for (size_t g = 0; g < kGroupCount; ++g) {
        group[g] = TestGroup::ConstructLoopbackMesh(num_hosts);
    }

    std::array<net::GroupPtr, kGroupCount> host_group = {
        { std::move(group[0][0]), std::move(group[1][0]) }
    };

    HostContext host_context(
        mem_config, std::move(host_group), workers_per_host);

    Context ctx(host_context, 0);
    common::NameThisThread("worker " + mem::to_string(my_host_rank));

    job_startpoint(ctx);
}

/******************************************************************************/
// Run() Variants for Different Net Backends

//! Run() implementation which uses a loopback net backend ("mock" or "tcp").
template <typename NetGroup>
static inline
int RunBackendLoopback(
    const char* backend, const std::function<void(Context&)>& job_startpoint) {

    char* endptr;

    // determine number of loopback hosts

    size_t num_hosts = std::thread::hardware_concurrency();

    const char* env_local = getenv("THRILL_LOCAL");
    if (env_local) {
        // parse envvar only if it exists.
        num_hosts = std::strtoul(env_local, &endptr, 10);

        if (!endptr || *endptr != 0 || num_hosts == 0) {
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
            num_hosts /= 2;
        }
    }

    // detect memory config

    MemoryConfig mem_config;
    if (mem_config.setup_detect() < 0) return -1;
    mem_config.print(workers_per_host);

    // okay, configuration is good.

    std::cerr << "Thrill: running locally with " << num_hosts
              << " test hosts and " << workers_per_host << " workers per host"
              << " in a local " << backend << " network." << std::endl;

    RunLoopbackThreads<NetGroup>(
        mem_config, num_hosts, workers_per_host, job_startpoint);

    return 0;
}

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
                  << " is required for tcp network backend."
                  << std::endl;
        return -1;
    }

    std::vector<std::string> hostlist;

    if (env_hostlist) {
        // first try to split by spaces, then by commas
        std::vector<std::string> list = common::Split(env_hostlist, ' ');

        if (list.size() == 1) {
            list = common::Split(env_hostlist, ',');
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
                  << " is required for tcp network backend."
                  << std::endl;
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
        workers_per_host = std::thread::hardware_concurrency();
    }

    // detect memory config

    MemoryConfig mem_config;
    if (mem_config.setup_detect() < 0) return -1;
    mem_config.print(workers_per_host);

    // okay, configuration is good.

    std::cerr << "Thrill: running in tcp network with " << hostlist.size()
              << " hosts and " << workers_per_host << " workers per host"
              << " as rank " << my_host_rank << " with endpoints";
    for (const std::string& ep : hostlist)
        std::cerr << ' ' << ep;
    std::cerr << std::endl;

    static const size_t kGroupCount = net::Manager::kGroupCount;

    // construct three TCP network groups
    std::array<std::unique_ptr<net::tcp::Group>, kGroupCount> groups;
    net::tcp::Construct(
        my_host_rank, hostlist, groups.data(), net::Manager::kGroupCount);

    std::array<net::GroupPtr, kGroupCount> host_groups = {
        { std::move(groups[0]), std::move(groups[1]) }
    };

    // construct HostContext
    HostContext host_context(
        mem_config, std::move(host_groups), workers_per_host);

    std::vector<std::thread> threads(workers_per_host);

    for (size_t worker = 0; worker < workers_per_host; worker++) {
        threads[worker] = std::thread(
            [&host_context, &job_startpoint, worker] {
                Context ctx(host_context, worker);
                common::NameThisThread("worker " + mem::to_string(worker));

                ctx.Launch(job_startpoint);
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

#if THRILL_HAVE_NET_MPI
static inline
int RunBackendMpi(const std::function<void(Context&)>& job_startpoint) {

    char* endptr;

    // detect memory config

    MemoryConfig mem_config;
    if (mem_config.setup_detect() < 0) return -1;
    mem_config.print();

    // determine number of local worker threads per MPI process

    size_t workers_per_host = 1;

    const char* env_workers_per_host = getenv("THRILL_WORKERS_PER_HOST");

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
        workers_per_host = std::thread::hardware_concurrency();
    }

    // detect memory config

    MemoryConfig mem_config;
    if (mem_config.setup_detect() < 0) return -1;
    mem_config.print(workers_per_host);

    // okay, configuration is good.

    size_t num_hosts = net::mpi::NumMpiProcesses();
    size_t mpi_rank = net::mpi::MpiRank();

    std::cerr << "Thrill: running in MPI network with " << num_hosts
              << " hosts and " << workers_per_host << " workers per host"
              << " as rank " << mpi_rank << "."
              << std::endl;

    static const size_t kGroupCount = net::Manager::kGroupCount;

    // construct three MPI network groups
    std::array<std::unique_ptr<net::mpi::Group>, kGroupCount> groups;
    net::mpi::Construct(num_hosts, groups.data(), kGroupCount);

    std::array<net::GroupPtr, kGroupCount> host_groups = {
        { std::move(groups[0]), std::move(groups[1]) }
    };

    // construct HostContext
    HostContext host_context(
        mem_config, std::move(host_groups), workers_per_host);

    // launch worker threads
    std::vector<std::thread> threads(workers_per_host);

    for (size_t worker = 0; worker < workers_per_host; worker++) {
        threads[worker] = std::thread(
            [&host_context, &job_startpoint, worker] {
                Context ctx(host_context, worker);
                common::NameThisThread("host " + mem::to_string(ctx.host_rank())
                                       + " worker " + mem::to_string(worker));

                ctx.Launch(job_startpoint);
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

static inline
int RunNotSupported(const char* env_net) {
    std::cerr << "Thrill: network backend " << env_net
              << " is not supported by this binary." << std::endl;
    return -1;
}

static inline
const char * DetectNetBackend() {
#if THRILL_HAVE_NET_MPI
    // detect openmpi run, add others as well.
    if (getenv("OMPI_COMM_WORLD_SIZE") != nullptr)
        return "mpi";
#endif
#if defined(_MSC_VER)
    return "mock";
#else
    const char* env_rank = getenv("THRILL_RANK");
    const char* env_hostlist = getenv("THRILL_HOSTLIST");

    if (env_rank || env_hostlist)
        return "tcp";
    else
        return "local";
#endif
}

//! Check environment variable THRILL_DIE_WITH_PARENT and enable process flag:
//! this is useful for ssh/invoke.sh: it kills spawned processes when the ssh
//! connection breaks. Hence: no more zombies.
static inline
int RunDieWithParent() {

    const char* env_die_with_parent = getenv("THRILL_DIE_WITH_PARENT");
    if (!env_die_with_parent || !*env_die_with_parent) return 0;

    char* endptr;

    long die_with_parent = std::strtol(env_die_with_parent, &endptr, 10);
    if (!endptr || *endptr != 0 ||
        (die_with_parent != 0 && die_with_parent != 1)) {
        std::cerr << "Thrill: environment variable"
                  << " THRILL_DIE_WITH_PARENT=" << env_die_with_parent
                  << " is not either 0 or 1."
                  << std::endl;
        return -1;
    }

    if (!die_with_parent) return 0;

#if __linux__
    if (prctl(PR_SET_PDEATHSIG, SIGTERM) != 0)
        throw common::ErrnoException("Error calling prctl(PR_SET_PDEATHSIG)");
    return 1;
#else
    std::cerr << "Thrill: DIE_WITH_PARENT is not supported on this platform.\n"
              << "Please submit a patch."
              << std::endl;
    return 0;
#endif
}

//! Check environment variable THRILL_UNLINK_BINARY and unlink given program
//! path: this is useful for ssh/invoke.sh: it removes the copied program files
//! _while_ it is running, hence it is gone even if the program crashes.
static inline
int RunUnlinkBinary() {

    const char* env_unlink_binary = getenv("THRILL_UNLINK_BINARY");
    if (!env_unlink_binary || !*env_unlink_binary) return 0;

    if (unlink(env_unlink_binary) != 0) {
        throw common::ErrnoException(
                  "Error calling unlink binary \""
                  + std::string(env_unlink_binary) + "\"");
    }

    return 0;
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

    if (RunDieWithParent() < 0)
        return -1;

    if (RunUnlinkBinary() < 0)
        return -1;

    // parse environment: THRILL_NET
    const char* env_net = getenv("THRILL_NET");

    // if no backend configured: automatically select one.
    if (!env_net) env_net = DetectNetBackend();

    // run with selected backend
    if (strcmp(env_net, "mock") == 0) {
        // mock network backend
        return RunBackendLoopback<net::mock::Group>("mock", job_startpoint);
    }

    if (strcmp(env_net, "local") == 0) {
#if THRILL_HAVE_NET_TCP
        // tcp loopback network backend
        return RunBackendLoopback<net::tcp::Group>("tcp", job_startpoint);
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

    if (strcmp(env_net, "mpi") == 0) {
#if THRILL_HAVE_NET_MPI
        // mpi network backend
        return RunBackendMpi(job_startpoint);
#else
        return RunNotSupported(env_net);
#endif
    }

    std::cerr << "Thrill: network backend " << env_net << " is unknown."
              << std::endl;
    return -1;
}

/******************************************************************************/
// MemoryConfig

void MemoryConfig::setup_test() {

    // set fixed amount of RAM for testing
    ram_ = 4 * 1024 * 1024 * 1024llu;

    // divide up ram_

    ram_block_pool_hard_ = ram_ / 3;
    ram_block_pool_soft_ = ram_block_pool_hard_ * 9 / 10;
    ram_dia_node_ = ram_ / 3;
    ram_floating_ = ram_ - ram_block_pool_hard_ - ram_dia_node_;

    // set memory limit, only BlockPool is excluded from malloc tracking
    mem::set_memory_limit_indication(ram_floating_ + ram_dia_node_);
}

int MemoryConfig::setup_detect() {

    // determine amount of physical RAM or take user's limit

    const char* env_ram = getenv("THRILL_RAM");

    if (env_ram && *env_ram) {
        uint64_t ram64;
        if (!common::ParseSiIecUnits(env_ram, ram64)) {
            std::cerr << "Thrill: environment variable"
                      << " THRILL_RAM=" << env_ram
                      << " is not a valid amount of RAM memory."
                      << std::endl;
            return -1;
        }
        ram_ = static_cast<size_t>(ram64);
    }
    else {
        // detect amount of physical memory on system
#if defined(_MSC_VER)
        MEMORYSTATUSEX memstx;
        memstx.dwLength = sizeof(memstx);
        GlobalMemoryStatusEx(&memstx);

        ram_ = memstx.ullTotalPhys;
#else
        ram_ = sysconf(_SC_PHYS_PAGES) * (size_t)sysconf(_SC_PAGESIZE);
#endif
    }

    // divide up ram_

    ram_block_pool_hard_ = ram_ / 3;
    ram_block_pool_soft_ = ram_block_pool_hard_ * 9 / 10;
    ram_dia_node_ = ram_ / 3;
    ram_floating_ = ram_ - ram_block_pool_hard_ - ram_dia_node_;

    // set memory limit, only BlockPool is excluded from malloc tracking
    mem::set_memory_limit_indication(ram_floating_ + ram_dia_node_);

    return 0;
}

MemoryConfig MemoryConfig::divide(size_t hosts) const {

    MemoryConfig mc = *this;
    mc.ram_ /= hosts;
    mc.ram_block_pool_hard_ /= hosts;
    mc.ram_block_pool_soft_ /= hosts;
    mc.ram_dia_node_ /= hosts;
    // free floating memory is not divided by host, as it is measured overall

    return mc;
}

void MemoryConfig::print(size_t workers_per_host) const {
    std::cerr
        << "Thrill: using "
        << common::FormatIecUnits(ram_) << "B RAM total,"
        << " BlockPool=" << common::FormatIecUnits(ram_block_pool_hard_) << "B,"
        << " DIANode="
        << common::FormatIecUnits(ram_dia_node_ / workers_per_host) << "B,"
        << " floating=" << common::FormatIecUnits(ram_floating_) << "B."
        << std::endl;
}

/******************************************************************************/
// Context methods

template <>
std::shared_ptr<data::CatStream> Context::GetNewStream<data::CatStream>() {
    return GetNewCatStream();
}

template <>
std::shared_ptr<data::MixStream> Context::GetNewStream<data::MixStream>() {
    return GetNewMixStream();
}

void Context::Launch(const std::function<void(Context&)>& job_startpoint) {
    logger_ << "class" << "Context"
            << "event" << "job-start";

    common::StatsTimerStart overall_timer;

    try {
        job_startpoint(*this);
    }
    catch (std::exception& e) {
        LOG1 << "worker " << my_rank() << " threw " << typeid(e).name();
        LOG1 << "  what(): " << e.what();

        logger_ << "class" << "Context"
                << "event" << "job-exception"
                << "exception" << typeid(e).name()
                << "what" << e.what();
        throw;
    }

    logger_ << "class" << "Context"
            << "event" << "job-done"
            << "elapsed" << overall_timer;

    net.Barrier();
}

/******************************************************************************/
// Log path creator

std::string HostContext::MakeHostLogPath(size_t host_rank) {
    const char* env_log = getenv("THRILL_LOG");
    if (!env_log) {
        if (host_rank == 0) {
            std::cerr << "Thrill: no THRILL_LOG was found, "
                      << "so no json log is written."
                      << std::endl;
        }
        return std::string();
    }

    std::string output = env_log;
    if (output == "/dev/stdout")
        return output;

    if (output == "")
        return std::string();

    return output + "-host-" + std::to_string(host_rank) + ".json";
}

std::string Context::MakeWorkerLogPath(size_t worker_rank) {
    const char* env_log = getenv("THRILL_LOG");
    if (!env_log) {
        // warning was already outputted for HostContext
        return std::string();
    }

    std::string output = env_log;
    if (output == "/dev/stdout")
        return output;

    if (output == "")
        return std::string();

    return output + "-worker-" + std::to_string(worker_rank) + ".json";
}

} // namespace api
} // namespace thrill

/******************************************************************************/
