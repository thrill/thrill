/*******************************************************************************
 * thrill/common/porting.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_PORTING_HEADER
#define THRILL_COMMON_PORTING_HEADER

#include <thrill/common/logger.hpp>

#if defined(_MSC_VER)
// windows/msvc is a mess.
#include <BaseTsd.h>
using ssize_t = SSIZE_T;
#endif

#include <string>
#include <system_error>
#include <thread>

namespace thrill {
namespace common {

//! set FD_CLOEXEC on file descriptor (if possible)
void PortSetCloseOnExec(int fd);

//! create a pair of pipe file descriptors
void MakePipe(int out_pipefds[2]);

//! try to figure out the command line options of the current process and log it
//! to json logger
void LogCmdlineParams(JsonLogger& logger);

//! create a std::thread and repeat creation if it fails
template <typename... Args>
std::thread CreateThread(Args&& ... args) {
    // try for 300 seconds
    size_t r = 3000;
    while (1) {
        try {
            return std::thread(std::forward<Args>(args) ...);
        }
        catch (std::system_error&) {
            if (--r == 0) throw;
            LOG1 << "Thread creation failed, retrying.";
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}

//! set cpu/core affinity of a thread
void SetCpuAffinity(std::thread& thread, size_t cpu_id);

//! set cpu/core affinity of current thread
void SetCpuAffinity(size_t cpu_id);

//! get hostname
std::string GetHostname();

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_PORTING_HEADER

/******************************************************************************/
