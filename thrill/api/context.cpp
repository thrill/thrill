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

#include <thrill/api/dia_base.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/linux_proc_stats.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/common/profile_thread.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/io/iostats.hpp>
#include <thrill/vfs/file_io.hpp>

#include <tlx/string/format_si_iec_units.hpp>
#include <tlx/string/parse_si_iec_units.hpp>
#include <tlx/string/split.hpp>

// mock net backend is always available -tb :)
#include <thrill/net/mock/group.hpp>

#if THRILL_HAVE_NET_TCP
#include <thrill/net/tcp/construct.hpp>
#endif

#if THRILL_HAVE_NET_MPI
#include <thrill/net/mpi/group.hpp>
#endif

#if THRILL_HAVE_NET_IB
#include <thrill/net/ib/group.hpp>
#endif

#if __linux__

// linux-specific process control
#include <sys/prctl.h>

// for calling getrlimit() to determine memory limit
#include <sys/resource.h>
#include <sys/time.h>

#endif

#if __APPLE__

// for sysctl()
#include <sys/sysctl.h>
#include <sys/types.h>

#endif

#if defined(_MSC_VER)

// for detecting amount of physical memory
#include <windows.h>

#endif

#include <algorithm>
#include <csignal>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
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

    static constexpr size_t kGroupCount = net::Manager::kGroupCount;

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
                h, mem_config, std::move(host_group), workers_per_host));
    }

    return host_context;
}

//! Generic runner for backends supporting loopback tests.
template <typename NetGroup>
static inline void
RunLoopbackThreads(
    const MemoryConfig& mem_config,
    size_t num_hosts, size_t workers_per_host, size_t core_offset,
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
            size_t id = host * workers_per_host + worker;
            threads[id] = common::CreateThread(
                [&host_contexts, &job_startpoint, host, worker, log_prefix] {
                    Context ctx(*host_contexts[host], worker);
                    common::NameThisThread(
                        log_prefix + " worker " + mem::to_string(worker));

                    ctx.Launch(job_startpoint);
                });
            common::SetCpuAffinity(threads[id], core_offset + id);
        }
    }

    // join worker threads
    for (size_t i = 0; i < num_hosts * workers_per_host; i++) {
        threads[i].join();
    }
}

/******************************************************************************/
// Other Configuration Initializations

static inline bool SetupBlockSize() {

    const char* env_block_size = getenv("THRILL_BLOCK_SIZE");
    if (!env_block_size || !*env_block_size) return true;

    char* endptr;
    data::default_block_size = std::strtoul(env_block_size, &endptr, 10);

    if (!endptr || *endptr != 0 || data::default_block_size == 0) {
        std::cerr << "Thrill: environment variable"
                  << " THRILL_BLOCK_SIZE=" << env_block_size
                  << " is not a valid number."
                  << std::endl;
        return false;
    }

    data::start_block_size = data::default_block_size;

    std::cerr << "Thrill: setting default_block_size = "
              << data::default_block_size
              << std::endl;

    return true;
}

static inline bool Initialize() {

    if (!SetupBlockSize()) return false;

    vfs::Initialize();

    return true;
}

static inline bool Deinitialize() {

    vfs::Deinitialize();

    return true;
}

/******************************************************************************/
// Constructions using TestGroup (either mock or tcp-loopback) for local testing

#if defined(_MSC_VER)
using TestGroup = net::mock::Group;
#else
using TestGroup = net::tcp::Group;
#endif

void RunLocalMock(const MemoryConfig& mem_config,
                  size_t num_hosts, size_t workers_per_host,
                  const std::function<void(Context&)>& job_startpoint) {

    return RunLoopbackThreads<TestGroup>(
        mem_config, num_hosts, workers_per_host, 0, job_startpoint);
}

std::vector<std::unique_ptr<HostContext> >
HostContext::ConstructLoopback(size_t num_hosts, size_t workers_per_host) {

    // set fixed amount of RAM for testing
    MemoryConfig mem_config;
    mem_config.setup(4 * 1024 * 1024 * 1024llu);
    mem_config.verbose_ = false;

    return ConstructLoopbackHostContexts<TestGroup>(
        mem_config, num_hosts, workers_per_host);
}

// Windows porting madness because setenv() is apparently dangerous
#if defined(_MSC_VER)
int wrap_setenv(const char* name, const char* value, int overwrite) {
    if (!overwrite) {
        size_t envsize = 0;
        int errcode = getenv_s(&envsize, nullptr, 0, name);
        if (errcode || envsize) return errcode;
    }
    return _putenv_s(name, value);
}
#else
int wrap_setenv(const char* name, const char* value, int overwrite) {
    return setenv(name, value, overwrite);
}
#endif

void RunLocalTests(const std::function<void(Context&)>& job_startpoint) {
    // set fixed amount of RAM for testing
    RunLocalTests(4 * 1024 * 1024 * 1024llu, job_startpoint);
}

void RunLocalTests(
    size_t ram, const std::function<void(Context&)>& job_startpoint) {

    // discard json log
    wrap_setenv("THRILL_LOG", "", /* overwrite */ 1);

    // set fixed amount of RAM for testing
    MemoryConfig mem_config;
    mem_config.verbose_ = false;
    mem_config.setup(ram);

    static constexpr size_t num_hosts_list[] = { 1, 2, 5, 8 };
    static constexpr size_t num_workers_list[] = { 1, 3 };

    for (const size_t& num_hosts : num_hosts_list) {
        for (const size_t& workers_per_host : num_workers_list) {
            RunLocalMock(mem_config, num_hosts, workers_per_host,
                         job_startpoint);
        }
    }
}

void RunLocalSameThread(const std::function<void(Context&)>& job_startpoint) {

    size_t my_host_rank = 0;
    size_t workers_per_host = 1;
    size_t num_hosts = 1;
    static constexpr size_t kGroupCount = net::Manager::kGroupCount;

    // set fixed amount of RAM for testing
    MemoryConfig mem_config;
    mem_config.verbose_ = false;
    mem_config.setup(4 * 1024 * 1024 * 1024llu);
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
        0, mem_config, std::move(host_group), workers_per_host);

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

    size_t num_hosts = 2;

    const char* env_local = getenv("THRILL_LOCAL");
    if (env_local && *env_local) {
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
    if (env_workers_per_host && *env_workers_per_host) {
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
        // distribute threads into hosts
        workers_per_host = std::thread::hardware_concurrency() / num_hosts;
    }

    // core offset for pinning
    const char* env_core_offset = getenv("THRILL_CORE_OFFSET");
    size_t core_offset = 0;
    if (env_core_offset && *env_core_offset) {
        core_offset = std::strtoul(env_core_offset, &endptr, 10);

        size_t last_core = core_offset + num_hosts * workers_per_host;
        if (!endptr || *endptr != 0 ||
            last_core > std::thread::hardware_concurrency())
        {
            std::cerr << "Thrill: environment variable"
                      << " THRILL_CORE_OFFSET=" << env_core_offset
                      << " is not a valid number of cores to skip for pinning."
                      << std::endl;
            return -1;
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

    if (!Initialize()) return -1;

    RunLoopbackThreads<NetGroup>(
        mem_config, num_hosts, workers_per_host, core_offset, job_startpoint);

    if (!Deinitialize()) return -1;

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

    if (!env_rank)
        env_rank = getenv("SLURM_PROCID");

    if (env_rank && *env_rank) {
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

    if (env_hostlist && *env_hostlist) {
        // first try to split by spaces, then by commas
        std::vector<std::string> list = tlx::split(' ', env_hostlist);

        if (list.size() == 1) {
            tlx::split(&list, ',', env_hostlist);
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

    if (env_workers_per_host && *env_workers_per_host) {
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
              << " with " << common::GetHostname()
              << " as rank " << my_host_rank << " and endpoints";
    for (const std::string& ep : hostlist)
        std::cerr << ' ' << ep;
    std::cerr << std::endl;

    if (!Initialize()) return -1;

    static constexpr size_t kGroupCount = net::Manager::kGroupCount;

    // construct three TCP network groups
    std::array<std::unique_ptr<net::tcp::Group>, kGroupCount> groups;
    net::tcp::Construct(
        my_host_rank, hostlist, groups.data(), net::Manager::kGroupCount);

    std::array<net::GroupPtr, kGroupCount> host_groups = {
        { std::move(groups[0]), std::move(groups[1]) }
    };

    // construct HostContext
    HostContext host_context(
        0, mem_config, std::move(host_groups), workers_per_host);

    std::vector<std::thread> threads(workers_per_host);

    for (size_t worker = 0; worker < workers_per_host; worker++) {
        threads[worker] = common::CreateThread(
            [&host_context, &job_startpoint, worker] {
                Context ctx(host_context, worker);
                common::NameThisThread("worker " + mem::to_string(worker));

                ctx.Launch(job_startpoint);
            });
        common::SetCpuAffinity(threads[worker], worker);
    }

    // join worker threads
    int global_result = 0;

    for (size_t i = 0; i < workers_per_host; i++) {
        threads[i].join();
    }

    if (!Deinitialize()) return -1;

    return global_result;
}
#endif

#if THRILL_HAVE_NET_MPI
static inline
int RunBackendMpi(const std::function<void(Context&)>& job_startpoint) {

    char* endptr;

    // determine number of local worker threads per MPI process

    size_t workers_per_host = 1;

    const char* env_workers_per_host = getenv("THRILL_WORKERS_PER_HOST");

    if (env_workers_per_host && *env_workers_per_host) {
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

    // reserve one thread for MPI net::Dispatcher which runs a busy-waiting loop

    die_unless(workers_per_host > 1);
    --workers_per_host;

    // detect memory config

    MemoryConfig mem_config;
    if (mem_config.setup_detect() < 0) return -1;
    mem_config.print(workers_per_host);

    // okay, configuration is good.

    size_t num_hosts = net::mpi::NumMpiProcesses();
    size_t mpi_rank = net::mpi::MpiRank();

    std::cerr << "Thrill: running in MPI network with " << num_hosts
              << " hosts and " << workers_per_host << "+1 workers per host"
              << " with " << common::GetHostname()
              << " as rank " << mpi_rank << "."
              << std::endl;

    if (!Initialize()) return -1;

    static constexpr size_t kGroupCount = net::Manager::kGroupCount;

    // construct three MPI network groups
    std::array<std::unique_ptr<net::mpi::Group>, kGroupCount> groups;
    net::mpi::Construct(num_hosts, groups.data(), kGroupCount);

    std::array<net::GroupPtr, kGroupCount> host_groups = {
        { std::move(groups[0]), std::move(groups[1]) }
    };

    // construct HostContext
    HostContext host_context(
        0, mem_config, std::move(host_groups), workers_per_host);

    // launch worker threads
    std::vector<std::thread> threads(workers_per_host);

    for (size_t worker = 0; worker < workers_per_host; worker++) {
        threads[worker] = common::CreateThread(
            [&host_context, &job_startpoint, worker] {
                Context ctx(host_context, worker);
                common::NameThisThread("host " + mem::to_string(ctx.host_rank())
                                       + " worker " + mem::to_string(worker));

                ctx.Launch(job_startpoint);
            });
        common::SetCpuAffinity(threads[worker], worker);
    }

    // join worker threads
    int global_result = 0;

    for (size_t i = 0; i < workers_per_host; i++) {
        threads[i].join();
    }

    if (!Deinitialize()) return -1;

    return global_result;
}
#endif

#if THRILL_HAVE_NET_IB
static inline
int RunBackendIb(const std::function<void(Context&)>& job_startpoint) {

    char* endptr;

    // determine number of local worker threads per IB/MPI process

    size_t workers_per_host = 1;

    const char* env_workers_per_host = getenv("THRILL_WORKERS_PER_HOST");

    if (env_workers_per_host && *env_workers_per_host) {
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

    size_t num_hosts = net::ib::NumMpiProcesses();
    size_t mpi_rank = net::ib::MpiRank();

    std::cerr << "Thrill: running in IB/MPI network with " << num_hosts
              << " hosts and " << workers_per_host << " workers per host"
              << " with " << common::GetHostname()
              << " as rank " << mpi_rank << "."
              << std::endl;

    if (!Initialize()) return -1;

    static constexpr size_t kGroupCount = net::Manager::kGroupCount;

    // construct two MPI network groups
    std::array<std::unique_ptr<net::ib::Group>, kGroupCount> groups;
    net::ib::Construct(num_hosts, groups.data(), kGroupCount);

    std::array<net::GroupPtr, kGroupCount> host_groups = {
        { std::move(groups[0]), std::move(groups[1]) }
    };

    // construct HostContext
    HostContext host_context(
        0, mem_config, std::move(host_groups), workers_per_host);

    // launch worker threads
    std::vector<std::thread> threads(workers_per_host);

    for (size_t worker = 0; worker < workers_per_host; worker++) {
        threads[worker] = common::CreateThread(
            [&host_context, &job_startpoint, worker] {
                Context ctx(host_context, worker);
                common::NameThisThread("host " + mem::to_string(ctx.host_rank())
                                       + " worker " + mem::to_string(worker));

                ctx.Launch(job_startpoint);
            });
        common::SetCpuAffinity(threads[worker], worker);
    }

    // join worker threads
    int global_result = 0;

    for (size_t i = 0; i < workers_per_host; i++) {
        threads[i].join();
    }

    if (!Deinitialize()) return -1;

    return global_result;
}
#endif

int RunNotSupported(const char* env_net) {
    std::cerr << "Thrill: network backend " << env_net
              << " is not supported by this binary." << std::endl;
    return -1;
}

static inline
const char * DetectNetBackend() {
    // detect openmpi run, add others as well.
    if (getenv("OMPI_COMM_WORLD_SIZE") != nullptr) {
#if THRILL_HAVE_NET_IB
        return "ib";
#elif THRILL_HAVE_NET_MPI
        return "mpi";
#else
        std::cerr << "Thrill: MPI environment detected, but network backend mpi"
                  << " is not supported by this binary." << std::endl;
        return nullptr;
#endif
    }
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

int RunCheckDieWithParent() {

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

int RunCheckUnlinkBinary() {

    const char* env_unlink_binary = getenv("THRILL_UNLINK_BINARY");
    if (!env_unlink_binary || !*env_unlink_binary) return 0;

    if (unlink(env_unlink_binary) != 0) {
        throw common::ErrnoException(
                  "Error calling unlink binary \""
                  + std::string(env_unlink_binary) + "\"");
    }

    return 0;
}

int Run(const std::function<void(Context&)>& job_startpoint) {

    if (RunCheckDieWithParent() < 0)
        return -1;

    if (RunCheckUnlinkBinary() < 0)
        return -1;

    // parse environment: THRILL_NET
    const char* env_net = getenv("THRILL_NET");

    // if no backend configured: automatically select one.
    if (!env_net || !*env_net) {
        env_net = DetectNetBackend();
        if (!env_net) return -1;
    }

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

    if (strcmp(env_net, "ib") == 0) {
#if THRILL_HAVE_NET_IB
        // ib/mpi network backend
        return RunBackendIb(job_startpoint);
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

void MemoryConfig::setup(size_t ram) {
    ram_ = ram;
    apply();
}

int MemoryConfig::setup_detect() {

    // determine amount of physical RAM or take user's limit

    const char* env_ram = getenv("THRILL_RAM");

    if (env_ram && *env_ram) {
        uint64_t ram64;
        if (!tlx::parse_si_iec_units(env_ram, &ram64)) {
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
#elif __APPLE__
        int mib[2];
        int64_t physical_memory;
        size_t length;

        // Get the physical memory size
        mib[0] = CTL_HW;
        mib[1] = HW_MEMSIZE;
        length = sizeof(physical_memory);
        sysctl(mib, 2, &physical_memory, &length, nullptr, 0);
        ram_ = static_cast<size_t>(physical_memory);
#else
        ram_ = sysconf(_SC_PHYS_PAGES) * (size_t)sysconf(_SC_PAGESIZE);
#endif

#if __linux__
        // use getrlimit() to check user limit on address space
        struct rlimit rl;
        if (getrlimit(RLIMIT_AS, &rl) == 0) {
            if (rl.rlim_cur != 0 && rl.rlim_cur * 3 / 4 < ram_) {
                ram_ = rl.rlim_cur * 3 / 4;
            }
        }
        else {
            sLOG1 << "getrlimit(): " << strerror(errno);
        }
#endif
    }

    apply();

    return 0;
}

void MemoryConfig::apply() {
    // divide up ram_

    ram_workers_ = ram_ / 3;
    ram_block_pool_hard_ = ram_ / 3;
    ram_block_pool_soft_ = ram_block_pool_hard_ * 9 / 10;
    ram_floating_ = ram_ - ram_block_pool_hard_ - ram_workers_;

    // set memory limit, only BlockPool is excluded from malloc tracking, as
    // only it uses bypassing allocators.
    mem::set_memory_limit_indication(ram_floating_ + ram_workers_);
}

MemoryConfig MemoryConfig::divide(size_t hosts) const {

    MemoryConfig mc = *this;
    mc.ram_ /= hosts;
    mc.ram_block_pool_hard_ /= hosts;
    mc.ram_block_pool_soft_ /= hosts;
    mc.ram_workers_ /= hosts;
    // free floating memory is not divided by host, as it is measured overall

    return mc;
}

void MemoryConfig::print(size_t workers_per_host) const {
    if (!verbose_) return;

    std::cerr
        << "Thrill: using "
        << tlx::format_iec_units(ram_) << "B RAM total,"
        << " BlockPool=" << tlx::format_iec_units(ram_block_pool_hard_) << "B,"
        << " workers="
        << tlx::format_iec_units(ram_workers_ / workers_per_host) << "B,"
        << " floating=" << tlx::format_iec_units(ram_floating_) << "B."
        << std::endl;
}

/******************************************************************************/
// HostContext methods

HostContext::HostContext(
    size_t local_host_id,
    const MemoryConfig& mem_config,
    std::array<net::GroupPtr, net::Manager::kGroupCount>&& groups,

    size_t workers_per_host)

    : mem_config_(mem_config),
      base_logger_(MakeHostLogPath(groups[0]->my_host_rank())),
      logger_(&base_logger_, "host_rank", groups[0]->my_host_rank()),
      profiler_(std::make_unique<common::ProfileThread>()),
      local_host_id_(local_host_id),
      workers_per_host_(workers_per_host),
      net_manager_(std::move(groups), logger_) {

    // write command line parameters to json log
    common::LogCmdlineParams(logger_);

    StartLinuxProcStatsProfiler(*profiler_, logger_);

    // run memory profiler only on local host 0 (especially for test runs)
    if (local_host_id == 0)
        mem::StartMemProfiler(*profiler_, logger_);
}

std::string HostContext::MakeHostLogPath(size_t host_rank) {
    const char* env_log = getenv("THRILL_LOG");
    if (!env_log) {
        if (host_rank == 0 && mem_config().verbose_) {
            std::cerr << "Thrill: no THRILL_LOG was found, "
                      << "so no json log is written."
                      << std::endl;
        }
        return std::string();
    }

    std::string output = env_log;
    if (output == "" || output == "-")
        return std::string();
    if (output == "/dev/stdout")
        return output;
    if (output == "stdout")
        return "/dev/stdout";

    return output + "-host-" + std::to_string(host_rank) + ".json";
}

/******************************************************************************/
// Context methods

data::File Context::GetFile(DIABase* dia) {
    return GetFile(dia ? dia->id() : 0);
}

data::FilePtr Context::GetFilePtr(size_t dia_id) {
    return tlx::make_counting<data::File>(
        block_pool_, local_worker_id_, dia_id);
}

data::FilePtr Context::GetFilePtr(DIABase* dia) {
    return GetFilePtr(dia ? dia->id() : 0);
}

data::CatStreamPtr Context::GetNewCatStream(size_t dia_id) {
    return multiplexer_.GetNewCatStream(local_worker_id_, dia_id);
}

data::CatStreamPtr Context::GetNewCatStream(DIABase* dia) {
    return GetNewCatStream(dia ? dia->id() : 0);
}

data::MixStreamPtr Context::GetNewMixStream(size_t dia_id) {
    return multiplexer_.GetNewMixStream(local_worker_id_, dia_id);
}

data::MixStreamPtr Context::GetNewMixStream(DIABase* dia) {
    return GetNewMixStream(dia ? dia->id() : 0);
}

template <>
tlx::CountingPtr<data::CatStream>
Context::GetNewStream<data::CatStream>(size_t dia_id) {
    return GetNewCatStream(dia_id);
}

template <>
tlx::CountingPtr<data::MixStream>
Context::GetNewStream<data::MixStream>(size_t dia_id) {
    return GetNewMixStream(dia_id);
}

struct OverallStats {

    //! overall run time
    double runtime;

    //! maximum ByteBlock allocation on all workers
    size_t max_block_bytes;

    //! network traffic performed by net layer
    size_t net_traffic_tx, net_traffic_rx;

    //! I/O volume performed by io layer
    size_t io_volume;

    //! maximum external memory allocation
    size_t io_max_allocation;

    friend std::ostream& operator << (std::ostream& os, const OverallStats& c) {
        return os << "[OverallStats"
                  << " runtime=" << c.runtime
                  << " max_block_bytes=" << c.max_block_bytes
                  << " net_traffic_tx=" << c.net_traffic_tx
                  << " net_traffic_rx=" << c.net_traffic_rx
                  << " io_volume=" << c.io_volume
                  << " io_max_allocation=" << c.io_max_allocation
                  << "]";
    }

    OverallStats operator + (const OverallStats& b) const {
        OverallStats r;
        r.runtime = std::max(runtime, b.runtime);
        r.max_block_bytes = max_block_bytes + b.max_block_bytes;
        r.net_traffic_tx = net_traffic_tx + b.net_traffic_tx;
        r.net_traffic_rx = net_traffic_rx + b.net_traffic_rx;
        r.io_volume = io_volume + b.io_volume;
        r.io_max_allocation = std::max(io_max_allocation, b.io_max_allocation);
        return r;
    }
};

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

    // wait for all local workers to finish, prior to closing multiplexer's
    // streams.
    net.LocalBarrier();

    if (my_rank() == 0)
        multiplexer_.Close();

    net.LocalBarrier();

    overall_timer.Stop();

    // collect overall statistics
    OverallStats stats;
    stats.runtime = overall_timer.SecondsDouble();

    stats.max_block_bytes =
        local_worker_id_ == 0 ? block_pool().max_total_bytes() : 0;

    std::tie(stats.net_traffic_tx, stats.net_traffic_rx) =
        local_worker_id_ == 0 ? net_manager_.Traffic()
        : std::pair<size_t, size_t>(0, 0);

    if (local_host_id_ == 0 && local_worker_id_ == 0) {
        io::StatsData io_stats(*io::Stats::GetInstance());
        stats.io_volume = io_stats.read_volume() + io_stats.write_volume();
        stats.io_max_allocation =
            io::BlockManager::GetInstance()->maximum_allocation();
    }
    else {
        stats.io_volume = 0;
        stats.io_max_allocation = 0;
    }

    LOG0 << stats;

    stats = net.Reduce(stats);

    if (my_rank() == 0) {
        using tlx::format_iec_units;

        if (stats.net_traffic_rx != stats.net_traffic_tx)
            LOG1 << "Manager::Traffic() tx/rx asymmetry = "
                 << common::abs_diff(stats.net_traffic_tx, stats.net_traffic_rx);

        if (mem_config().verbose_) {
            std::cerr
                << "Thrill:"
                << " ran " << stats.runtime << "s with max "
                << format_iec_units(stats.max_block_bytes) << "B in DIA Blocks, "
                << format_iec_units(stats.net_traffic_tx) << "B network traffic, "
                << format_iec_units(stats.io_volume) << "B disk I/O, and "
                << format_iec_units(stats.io_max_allocation) << "B max disk use."
                << std::endl;
        }

        logger_ << "class" << "Context"
                << "event" << "summary"
                << "runtime" << stats.runtime
                << "net_traffic" << stats.net_traffic_tx
                << "io_volume" << stats.io_volume
                << "io_max_allocation" << stats.io_max_allocation;
    }
}

} // namespace api
} // namespace thrill

/******************************************************************************/
