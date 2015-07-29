/*******************************************************************************
 * c7a/common/logger.cpp
 *
 * Simple and less simple logging classes.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/common/logger.hpp>

#include <iomanip>

namespace c7a {
namespace common {

std::mutex Logger<true>::mutex_;

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
