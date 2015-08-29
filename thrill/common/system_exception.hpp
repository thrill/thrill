/*******************************************************************************
 * thrill/common/system_exception.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_SYSTEM_EXCEPTION_HEADER
#define THRILL_COMMON_SYSTEM_EXCEPTION_HEADER

#include <cstring>
#include <stdexcept>
#include <string>

namespace thrill {
namespace common {

/*!
 * An Exception which is thrown on system errors and contains errno information.
 */
class SystemException : public std::runtime_error
{
public:
    explicit SystemException(const std::string& what)
        : std::runtime_error(what)
    { }

    SystemException(const std::string& what, int _errno)
        : std::runtime_error(
              what + ": [" + std::to_string(_errno) + "] " + strerror(_errno))
    { }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_SYSTEM_EXCEPTION_HEADER

/******************************************************************************/
