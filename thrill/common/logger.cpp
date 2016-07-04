/*******************************************************************************
 * thrill/common/logger.cpp
 *
 * Simple and less simple logging classes.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/mem/manager.hpp>

#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

namespace thrill {
namespace common {

/******************************************************************************/

//! memory manager singleton for Logger
mem::Manager g_logger_mem_manager(nullptr, "Logger");

using StringCount = std::pair<mem::by_string, size_t>;

template <typename Type>
using LoggerAllocator = mem::FixedAllocator<Type, g_logger_mem_manager>;

//! deque without malloc tracking
template <typename Key, typename T, typename Compare = std::less<Key> >
using logger_map = std::map<Key, T, Compare,
                            LoggerAllocator<std::pair<const Key, T> > >;

//! mutex for s_threads map
static std::mutex s_mutex;

//! map thread id -> (name, message counter)
static logger_map<std::thread::id, StringCount> s_threads;

//! Defines a name for the current thread, only if no name was set previously
void NameThisThread(const mem::by_string& name) {
    std::lock_guard<std::mutex> lock(s_mutex);
    s_threads[std::this_thread::get_id()] = StringCount(name, 0);
}

//! Outputs the name of the current thread or 'unknown [id]'
void FormatNameForThisThread(std::ostream& os) {
    std::lock_guard<std::mutex> lock(s_mutex);

    auto it = s_threads.find(std::this_thread::get_id());
    if (it != s_threads.end()) {
        StringCount& sc = it->second;
        if (true) {
            std::ios::fmtflags flags(os.flags());
            // print "name #msg";
            os << sc.first << ' '
               << std::setfill('0') << std::setw(6) << sc.second++;
            os.flags(flags);
        }
        else {
            os << sc.first;
        }
    }
    else {
        os << "unknown " << std::this_thread::get_id();
    }
}

//! Returns the name of the current thread or 'unknown [id]'
std::string GetNameForThisThread() {
    std::ostringstream oss;
    FormatNameForThisThread(oss);
    return oss.str();
}

/******************************************************************************/

//! mutex for log output
static std::mutex s_logger_mutex;

void Logger::Output(const char* str) {
    // lock the global mutex of logger for serialized output in
    // multi-threaded programs.
    std::unique_lock<std::mutex> lock(s_logger_mutex);
    std::cout << str;
}

void Logger::Output(const std::string& str) {
    // lock the global mutex of logger for serialized output in
    // multi-threaded programs.
    std::unique_lock<std::mutex> lock(s_logger_mutex);
    std::cout << str;
}

void Logger::Output(const mem::safe_string& str) {
    // lock the global mutex of logger for serialized output in
    // multi-threaded programs.
    std::unique_lock<std::mutex> lock(s_logger_mutex);
    std::cout << str;
}

Logger::Logger() {
    oss_ << '[';
    FormatNameForThisThread(oss_);
    oss_ << "] ";
}

Logger::~Logger() {
    oss_ << '\n';
    Output(oss_.str());
}

SpacingLogger::SpacingLogger() {
    oss_ << '[';
    FormatNameForThisThread(oss_);
    oss_ << "] ";
}

SpacingLogger::~SpacingLogger() {
    oss_ << '\n';
    Logger::Output(oss_.str());
}

} // namespace common
} // namespace thrill

/******************************************************************************/
