/*******************************************************************************
 * misc/standalone_profiler.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <thrill/common/json_logger.hpp>
#include <thrill/common/linux_proc_stats.hpp>
#include <thrill/common/profile_thread.hpp>
#include <tlx/cmdline_parser.hpp>

#include <csignal>
#include <iostream>
#include <string>

using namespace thrill; // NOLINT

static bool s_terminate = false;

//! signal handler to catch CTRL+C
void sig_int_handler(int signum) {
    std::cerr << "Caught CTRL+C, terminating..." << std::endl;
    s_terminate = true;
    signal(signum, sig_int_handler);
}

int main(int argc, char* argv[]) {
    tlx::CmdlineParser clp;
    clp.set_description("Standalone Linux /proc JsonLogger from Thrill");

    size_t check_pid = 0;
    clp.add_size_t('p', "pid", check_pid, "Terminate when pid is not running.");

    std::string output;
    clp.add_param_string("output", output, "json logger output");

    if (!clp.process(argc, argv)) return -1;

    signal(SIGINT, sig_int_handler);

    // check some environment variables
    if (api::RunCheckDieWithParent() < 0) return -1;
    if (api::RunCheckUnlinkBinary() < 0) return -1;

    common::JsonLogger logger(output);

    {
        // starts profiler thread
        common::ProfileThread profiler;
        StartLinuxProcStatsProfiler(profiler, logger);

        while (!s_terminate) {
            if (check_pid) {
                // check if /proc/<pid>/cmdline exists, else break
                std::string path =
                    "/proc/" + std::to_string(check_pid) + "/cmdline";
                std::ifstream in(path.c_str());
                if (!in.good()) break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // joins profiler thread
    }

    return 0;
}

/******************************************************************************/
