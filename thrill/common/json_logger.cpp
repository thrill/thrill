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

#include <thrill/common/json_logger.hpp>
#include <thrill/common/logger.hpp>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>

namespace thrill {
namespace common {

/******************************************************************************/
// JsonLogger

JsonLogger::JsonLogger(const std::string& path) {
    if (!path.size()) return;

    os_.open(path.c_str());
    if (!os_.good()) {
        die("Could not open json log output: "
            << path << " : " << strerror(errno));
    }
}

JsonLine JsonLogger::line() {
    if (super_) {
        JsonLine out = super_->line();

        // append common key:value pairs
        if (common_.str_.size())
            out << common_;

        return out;
    }

    os_ << '{';

    JsonLine out(this, os_);

    // output timestamp in microseconds
    out << "ts"
        << std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // append common key:value pairs
    if (common_.str_.size())
        out << common_;

    return out;
}

} // namespace common
} // namespace thrill

/******************************************************************************/
