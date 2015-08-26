/*******************************************************************************
 * thrill/common/logger.cpp
 *
 * Simple and less simple logging classes.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/logger.hpp>

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

using StringCount = std::pair<mem::string, size_t>;

//! deque without malloc tracking
template <class Key, class T, class Compare = std::less<Key> >
using bypass_map = std::map<Key, T, Compare,
                            mem::BypassAllocator<std::pair<const Key, T> > >;

//! mutex for threads_ map
static std::mutex mutex_;

//! map thread id -> (name, message counter)
static bypass_map<std::thread::id, StringCount> threads_;

//! Defines a name for the current thread, only if no name was set previously
void NameThisThread(const mem::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    threads_[std::this_thread::get_id()] = StringCount(name, 0);
}

//! Outputs the name of the current thread or 'unknown [id]'
void FormatNameForThisThread(std::ostream& os) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = threads_.find(std::this_thread::get_id());
    if (it != threads_.end()) {
        StringCount& sc = it->second;
        if (true) {
            // print "name #msg";
            os << sc.first << ' '
               << std::setfill('0') << std::setw(6) << sc.second++;
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

//! mutex for threads_ map
static std::mutex logger_mutex_;

//! constructor: if real = false the output is suppressed.
Logger<true>::Logger() : oss_(&buf_) {
    oss_ << '[';
    FormatNameForThisThread(oss_);
    oss_ << "] ";
}

//! destructor: output a newline
Logger<true>::~Logger() {
    oss_ << '\n';
    // lock the global mutex of logger for serialized output in
    // multi-threaded programs.
    std::unique_lock<std::mutex> lock(logger_mutex_);
    oss_.flush();
    mem::string out = buf_.str();
    std::cout.write(out.data(), out.size());
}

//! constructor: if real = false the output is suppressed.
SpacingLogger<true>::SpacingLogger() : oss_(&buf_) {
    oss_ << '[';
    FormatNameForThisThread(oss_);
    oss_ << "] ";
}

//! destructor: output a newline
SpacingLogger<true>::~SpacingLogger() {
    oss_ << '\n';
    // lock the global mutex of logger for serialized output in
    // multi-threaded programs.
    std::unique_lock<std::mutex> lock(logger_mutex_);
    oss_.flush();
    mem::string out = buf_.str();
    std::cout.write(out.data(), out.size());
}

} // namespace common
} // namespace thrill

/******************************************************************************/
