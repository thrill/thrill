/*******************************************************************************
 * thrill/common/porting.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/defines.hpp>
#include <thrill/common/json_logger.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>

#include <tlx/string/replace.hpp>
#include <tlx/unused.hpp>

#include <fcntl.h>

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#if !defined(_MSC_VER)

#include <unistd.h>

#else

#include <io.h>

#endif

namespace thrill {
namespace common {

void PortSetCloseOnExec(int fd) {
#if !defined(_MSC_VER)
    if (fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) {
        throw ErrnoException("Error setting FD_CLOEXEC on file descriptor");
    }
#else
    tlx::unused(fd);
#endif
}

void MakePipe(int out_pipefds[2]) {
#if THRILL_HAVE_PIPE2
    if (pipe2(out_pipefds, O_CLOEXEC) != 0)
        throw ErrnoException("Error creating pipe");
#elif defined(_MSC_VER)
    if (_pipe(out_pipefds, 256, O_BINARY) != 0)
        throw ErrnoException("Error creating pipe");
#else
    if (pipe(out_pipefds) != 0)
        throw ErrnoException("Error creating pipe");

    PortSetCloseOnExec(out_pipefds[0]);
    PortSetCloseOnExec(out_pipefds[1]);
#endif
}

void LogCmdlineParams(JsonLogger& logger) {
#if __linux__
    // read cmdline from /proc/<pid>/cmdline
    pid_t mypid = getpid();

    std::ifstream proc("/proc/" + std::to_string(mypid) + "/cmdline");
    if (!proc.good()) return;

    std::vector<std::string> args;
    std::string arg;
    while (std::getline(proc, arg, '\0'))
        args.emplace_back(arg);

    std::string prog;
    if (!args.empty()) {
        prog = args[0];
        std::string::size_type slashpos = prog.rfind('/');
        if (slashpos != std::string::npos)
            prog = prog.substr(slashpos + 1);
    }

    std::ostringstream cmdline;
    for (size_t i = 0; i < args.size(); ++i) {
        if (i != 0)
            cmdline << ' ';

        arg = args[i];
        // escape " -> \"
        if (arg.find('"') != std::string::npos)
            tlx::replace_all(arg, "\"", "\\\"");
        cmdline << arg;
    }

    logger << "class" << "Cmdline"
           << "event" << "start"
           << "program" << prog
           << "argv" << args
           << "cmdline" << cmdline.str();
#else
    tlx::unused(logger);
#endif
}

void SetCpuAffinity(std::thread& thread, size_t cpu_id) {
#if __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id % std::thread::hardware_concurrency(), &cpuset);
    int rc = pthread_setaffinity_np(
        thread.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        LOG1 << "Error calling pthread_setaffinity_np(): "
             << rc << ": " << strerror(errno);
    }
#else
    tlx::unused(thread);
    tlx::unused(cpu_id);
#endif
}

void SetCpuAffinity(size_t cpu_id) {
#if __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id % std::thread::hardware_concurrency(), &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        LOG1 << "Error calling pthread_setaffinity_np(): "
             << rc << ": " << strerror(errno);
    }
#else
    tlx::unused(cpu_id);
#endif
}

std::string GetHostname() {
#if __linux__
    char buffer[64];
    gethostname(buffer, 64);
    return buffer;
#else
    return "<unknown host>";
#endif
}

} // namespace common
} // namespace thrill

/******************************************************************************/
