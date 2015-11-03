/*******************************************************************************
 * thrill/net/exception.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_EXCEPTION_HEADER
#define THRILL_NET_EXCEPTION_HEADER

#include <cstring>
#include <stdexcept>
#include <string>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

/*!
 * A Exception is thrown by Connection on all errors instead of returning
 * error codes. If ever we manage to recover from network errors, we probably
 * have to rebuild most of the network objects anyway.
 */
class Exception : public std::runtime_error
{
public:
    explicit Exception(const std::string& what)
        : std::runtime_error(what)
    { }

    Exception(const std::string& what, int _errno)
        : std::runtime_error(
              what + ": [" + std::to_string(_errno) + "] " + strerror(_errno))
    { }
};

// \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_EXCEPTION_HEADER

/******************************************************************************/
