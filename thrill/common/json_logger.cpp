/*******************************************************************************
 * thrill/common/json_logger.cpp
 *
 * Logger for statistical output in JSON format for post-processing.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/die.hpp>
#include <thrill/common/json_logger.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <vector>

namespace thrill {
namespace common {

/******************************************************************************/
// JsonLogger

JsonLogger::JsonLogger(const std::string& path) {
    if (path.empty()) return;

    os_.open(path.c_str());
    if (!os_.good()) {
        die("Could not open json log output: "
            << path << " : " << strerror(errno));
    }
}

JsonLogger::JsonLogger(JsonLogger* super)
    : super_(super) { }

JsonLine JsonLogger::line() {
    if (super_ != nullptr) {
        JsonLine out = super_->line();

        // append common key:value pairs
        if (!common_.str_.empty())
            out << common_;

        return out;
    }

    JsonLine out(this, os_);
    os_ << '{';

    // output timestamp in microseconds
    out << "ts"
        << std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // append common key:value pairs
    if (!common_.str_.empty())
        out << common_;

    return out;
}

} // namespace common
} // namespace thrill

/******************************************************************************/
