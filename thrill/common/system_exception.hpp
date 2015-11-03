/*******************************************************************************
 * thrill/common/system_exception.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_SYSTEM_EXCEPTION_HEADER
#define THRILL_COMMON_SYSTEM_EXCEPTION_HEADER

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

namespace thrill {
namespace common {

/*!
 * An Exception which is thrown on system errors.
 */
class SystemException : public std::runtime_error
{
public:
    explicit SystemException(const std::string& what)
        : std::runtime_error(what) { }
};

/*!
 * An Exception which is thrown on system errors and contains errno information.
 */
class ErrnoException : public SystemException
{
public:
    ErrnoException(const std::string& what, int _errno)
        : SystemException(
              what + ": [" + std::to_string(_errno) + "] " + strerror(_errno))
    { }

    explicit ErrnoException(const std::string& what)
        : ErrnoException(what, -1) { }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_SYSTEM_EXCEPTION_HEADER

/******************************************************************************/
