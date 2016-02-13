/*******************************************************************************
 * thrill/io/error_handling.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2007-2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_ERROR_HANDLING_HEADER
#define THRILL_IO_ERROR_HANDLING_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/config.hpp>
#include <thrill/io/exceptions.hpp>

#include <cerrno>
#include <cstring>
#include <sstream>

namespace thrill {
namespace io {

#if THRILL_MSVC
 #define THRILL_PRETTY_FUNCTION_NAME __FUNCTION__
#else
 #define THRILL_PRETTY_FUNCTION_NAME __PRETTY_FUNCTION__
#endif

////////////////////////////////////////////////////////////////////////////

//! Throws exception_type with "Error in [location] : [error_message]"
#define THRILL_THROW2(exception_type, location, error_message)    \
    do {                                                          \
        std::ostringstream msg;                                   \
        msg << "Error in " << location << " : " << error_message; \
        throw exception_type(msg.str());                          \
    } while (false)

//! Throws exception_type with "Error in [function] : [error_message]"
#define THRILL_THROW(exception_type, error_message) \
    THRILL_THROW2(exception_type,                   \
                  THRILL_PRETTY_FUNCTION_NAME,      \
                  error_message)

//! Throws exception_type with "Error in [function] : [error_message] : [errno_value message]"
#define THRILL_THROW_ERRNO2(exception_type, error_message, errno_value) \
    THRILL_THROW2(exception_type,                                       \
                  THRILL_PRETTY_FUNCTION_NAME,                          \
                  error_message << " : " << strerror(errno_value))

//! Throws exception_type with "Error in [function] : [error_message] : [errno message]"
#define THRILL_THROW_ERRNO(exception_type, error_message) \
    THRILL_THROW_ERRNO2(exception_type, error_message, errno)

//! Throws std::invalid_argument with "Error in [function] : [error_message]"
#define THRILL_THROW_INVALID_ARGUMENT(error_message) \
    THRILL_THROW2(std::invalid_argument,             \
                  THRILL_PRETTY_FUNCTION_NAME,       \
                  error_message)

//! Throws stxxl::unreachable with "Error in file [file], line [line] : this code should never be reachable"
#define THRILL_THROW_UNREACHABLE()                              \
    THRILL_THROW2(stxxl::unreachable,                           \
                  "file " << __FILE__ << ", line " << __LINE__, \
                  "this code should never be reachable")

////////////////////////////////////////////////////////////////////////////

//! Throws exception_type if (expr) with "Error in [function] : [error_message]"
#define THRILL_THROW_IF(expr, exception_type, error_message) \
    do {                                                     \
        if (expr) {                                          \
            THRILL_THROW(exception_type, error_message);     \
        }                                                    \
    } while (false)

//! Throws exception_type if (expr != 0) with "Error in [function] : [error_message]"
#define THRILL_THROW_NE_0(expr, exception_type, error_message) \
    THRILL_THROW_IF((expr) != 0, exception_type, error_message)

//! Throws exception_type if (expr == 0) with "Error in [function] : [error_message]"
#define THRILL_THROW_EQ_0(expr, exception_type, error_message) \
    THRILL_THROW_IF((expr) == 0, exception_type, error_message)

//! Throws exception_type if (expr < 0) with "Error in [function] : [error_message]"
#define THRILL_THROW_LT_0(expr, exception_type, error_message) \
    THRILL_THROW_IF((expr) < 0, exception_type, error_message)

////////////////////////////////////////////////////////////////////////////

//! Throws exception_type if (expr) with "Error in [function] : [error_message] : [errno message]"
#define THRILL_THROW_ERRNO_IF(expr, exception_type, error_message) \
    do {                                                           \
        if (expr) {                                                \
            THRILL_THROW_ERRNO(exception_type, error_message);     \
        }                                                          \
    } while (false)

//! Throws exception_type if (expr != 0) with "Error in [function] : [error_message] : [errno message]"
#define THRILL_THROW_ERRNO_NE_0(expr, exception_type, error_message) \
    THRILL_THROW_ERRNO_IF((expr) != 0, exception_type, error_message)

//! Throws exception_type if (expr == 0) with "Error in [function] : [error_message] : [errno message]"
#define THRILL_THROW_ERRNO_EQ_0(expr, exception_type, error_message) \
    THRILL_THROW_ERRNO_IF((expr) == 0, exception_type, error_message)

//! Throws exception_type if (expr < 0) with "Error in [function] : [error_message] : [errno message]"
#define THRILL_THROW_ERRNO_LT_0(expr, exception_type, error_message) \
    THRILL_THROW_ERRNO_IF((expr) < 0, exception_type, error_message)

////////////////////////////////////////////////////////////////////////////

//! Checks pthread call, if return != 0, throws stxxl::resource_error with "Error in [function] : [pthread_expr] : [errno message]
#define THRILL_CHECK_PTHREAD_CALL(expr)                             \
    do {                                                            \
        int res = (expr);                                           \
        if (res != 0) {                                             \
            THRILL_THROW_ERRNO2(stxxl::resource_error, #expr, res); \
        }                                                           \
    } while (false)

////////////////////////////////////////////////////////////////////////////

#if THRILL_WINDOWS || defined(__MINGW32__)

//! Throws exception_type with "Error in [function] : [error_message] : [formatted GetLastError()]"
#define THRILL_THROW_WIN_LASTERROR(exception_type, error_message)               \
    do {                                                                        \
        LPVOID lpMsgBuf;                                                        \
        DWORD dw = GetLastError();                                              \
        FormatMessage(                                                          \
            FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,        \
            nullptr, dw,                                                        \
            MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),                          \
            (LPTSTR)&lpMsgBuf,                                                  \
            0, nullptr);                                                        \
        std::ostringstream msg;                                                 \
        msg << "Error in " << THRILL_PRETTY_FUNCTION_NAME                       \
            << " : " << error_message                                           \
            << " : error code " << dw << " : " << static_cast<char*>(lpMsgBuf); \
        LocalFree(lpMsgBuf);                                                    \
        throw exception_type(msg.str());                                        \
    } while (false)

#endif

////////////////////////////////////////////////////////////////////////////

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_ERROR_HANDLING_HEADER

/******************************************************************************/
