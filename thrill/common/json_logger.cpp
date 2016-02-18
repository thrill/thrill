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

//! open JsonLogger with ofstream
JsonLogger::JsonLogger(const std::string& path) {
    if (!path.size()) return;

    os_.open(path.c_str());
    if (!os_.good()) {
        die("Could not open json log output: "
            << path << " : " << strerror(errno));
    }
}

JsonLine JsonLogger::line() {
    os_ << '{';
    elements_ = 0;

    JsonLine out(*this);
    out << "ts"
        << std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    return out;
}

} // namespace common
} // namespace thrill

/******************************************************************************/
