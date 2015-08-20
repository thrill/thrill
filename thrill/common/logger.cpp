/*******************************************************************************
 * thrill/common/logger.cpp
 *
 * Simple and less simple logging classes.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/logger.hpp>

#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

namespace c7a {
namespace common {

static std::mutex logger_mutex_;

//! destructor: output a newline
Logger<true>::~Logger() {
    oss_ << "\n";
    // lock the global mutex of logger for serialized output in
    // multi-threaded programs.
    std::unique_lock<std::mutex> lock(logger_mutex_);
    std::cout << oss_.str();
    std::cout.flush();
}

//! destructor: output a newline
SpacingLogger<true>::~SpacingLogger() {
    oss_ << "\n";
    // lock the global mutex of logger for serialized output in
    // multi-threaded programs.
    std::unique_lock<std::mutex> lock(logger_mutex_);
    std::cout << oss_.str();
    std::cout.flush();
}

/******************************************************************************/

using StringCount = std::pair<std::string, size_t>;

//! map thread id -> (name, message counter)
static std::map<std::thread::id, StringCount> threads_;
static std::mutex mutex_;

//! Defines a name for the current thread, only if no name was set previously
void NameThisThread(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    threads_[std::this_thread::get_id()] = StringCount(name, 0);
}

//! Returns the name of the current thread or 'unknown [id]'
std::string GetNameForThisThread() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = threads_.find(std::this_thread::get_id());
    if (it != threads_.end()) {
        StringCount& sc = it->second;
        if (true) {
            std::ostringstream ss;
            // print "name #msg";
            ss << sc.first << ' '
               << std::setfill('0') << std::setw(6) << sc.second++;
            return ss.str();
        }
        else {
            return sc.first;
        }
    }
    std::ostringstream ss;
    ss << "unknown " << std::this_thread::get_id();
    return ss.str();
}

} // namespace common
} // namespace c7a

/******************************************************************************/
