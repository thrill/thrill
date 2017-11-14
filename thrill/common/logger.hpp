/*******************************************************************************
 * thrill/common/logger.hpp
 *
 * Simple and less simple logging classes.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_LOGGER_HEADER
#define THRILL_COMMON_LOGGER_HEADER

#include <thrill/mem/allocator.hpp>
#include <thrill/mem/pool.hpp>

#include <array>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace thrill {
namespace common {

//! memory manager singleton for Logger
extern mem::Manager g_logger_mem_manager;

//! Defines a name for the current thread.
void NameThisThread(const mem::by_string& name);

//! Returns the name of the current thread or 'unknown [id]'
std::string GetNameForThisThread();

/******************************************************************************/

/*!

\brief LOG and sLOG for development and debugging

This is a short description of how to use \ref LOG and \ref sLOG for rapid
development of modules with debug output, and how to **keep it afterwards**.

There are two classes Logger and SpacingLogger, but one does not use these
directly.

Instead there are the macros: \ref LOG and \ref sLOG that can be used as such:
\code
LOG << "This will be printed with a newline";
sLOG << "Print variables a" << a << "b" << b << "c" << c;
\endcode

There macros only print the lines if the boolean variable **debug** is
true. This variable is searched for in the scope of the LOG, which means it can
be set or overridden in the function scope, the class scope, from **inherited
classes**, or even the global scope.

\code
class MyClass
{
    static constexpr bool debug = true;

    void func1()
    {
        LOG << "Hello World";

        LOG0 << "This is temporarily disabled.";
    }

    void func2()
    {
        static constexpr bool debug = false;
        LOG << "This is not printed any more.";

        LOG1 << "But this is forced.";
    }
};
\endcode

There are two variation of \ref LOG and \ref sLOG : append 0 or 1 for
temporarily disabled or enabled debug lines. These macros are then \ref LOG0,
\ref LOG1, \ref sLOG0, and \ref sLOG1. The suffix overrides the debug variable's
setting.

After a module works as intended, one can just set `debug = false`, and all
debug output will disappear.

## Critique of LOG and sLOG

The macros are only for rapid module-based development. It cannot be used as an
extended logging system for our network framework, where logs of network
execution and communication are collected for later analysis. Something else is
needed here.

 */
class Logger
{
private:
    //! collector stream
    mem::safe_ostringstream oss_;

public:
    //! mutex synchronized output to std::cout
    static void Output(const char* str);
    //! mutex synchronized output to std::cout
    static void Output(const std::string& str);
    //! mutex synchronized output to std::cout
    static void Output(const mem::safe_string& str);

    Logger();

    //! output any type, including io manipulators
    template <typename AnyType>
    Logger& operator << (const AnyType& at) {
        oss_ << at;
        return *this;
    }

    //! destructor: output a newline
    ~Logger();
};

/*!
 * A logging class which outputs spaces between elements pushed via
 * operator<<. Depending on the real parameter the output may be suppressed.
 */
class SpacingLogger
{
private:
    //! true until the first element it outputted.
    bool first_ = true;

    //! collector stream
    mem::safe_ostringstream oss_;

public:
    SpacingLogger();

    //! output any type, including io manipulators
    template <typename AnyType>
    SpacingLogger& operator << (const AnyType& at) {
        if (!first_) oss_ << ' ';
        else first_ = false;

        oss_ << at;

        return *this;
    }

    //! destructor: output a newline
    ~SpacingLogger();
};

class LoggerVoidify
{
public:
    void operator & (Logger&) { }
    void operator & (SpacingLogger&) { }
};

//! Explicitly specify the condition for logging
#define LOGC(cond)      \
    !(cond) ? (void)0 : \
    ::thrill::common::LoggerVoidify() & ::thrill::common::Logger()

//! Default logging method: output if the local debug variable is true.
#define LOG LOGC(debug)

//! Override default output: never or always output log.
#define LOG0 LOGC(false)
#define LOG1 LOGC(true)

//! Explicitly specify the condition for logging
#define sLOGC(cond)     \
    !(cond) ? (void)0 : \
    ::thrill::common::LoggerVoidify() & ::thrill::common::SpacingLogger()

//! Default logging method: output if the local debug variable is true.
#define sLOG sLOGC(debug)

//! Override default output: never or always output log.
#define sLOG0 sLOGC(false)
#define sLOG1 sLOGC(true)

/******************************************************************************/
// Nice LOG/sLOG Formatters for std::pair, std::tuple, std::vector, and
// std::array types

using log_stream = mem::safe_ostringstream;

template <typename A, typename B>
log_stream& operator << (log_stream& os, const std::pair<A, B>& p) {
    os << '(' << p.first << ',' << p.second << ')';
    return os;
}

static inline log_stream& operator << (log_stream& os, const std::tuple<>&) {
    os << '(' << ')';
    return os;
}

template <typename A>
log_stream& operator << (log_stream& os, const std::tuple<A>& t) {
    os << '(' << std::get<0>(t) << ')';
    return os;
}

template <typename A, typename B>
log_stream& operator << (log_stream& os, const std::tuple<A, B>& t) {
    os << '(' << std::get<0>(t) << ',' << std::get<1>(t) << ')';
    return os;
}

template <typename A, typename B, typename C>
log_stream& operator << (log_stream& os, const std::tuple<A, B, C>& t) {
    os << '(' << std::get<0>(t) << ',' << std::get<1>(t)
       << ',' << std::get<2>(t) << ')';
    return os;
}

template <typename A, typename B, typename C, typename D>
log_stream& operator << (log_stream& os, const std::tuple<A, B, C, D>& t) {
    os << '(' << std::get<0>(t) << ',' << std::get<1>(t)
       << ',' << std::get<2>(t) << ',' << std::get<3>(t) << ')';
    return os;
}

template <typename A, typename B, typename C, typename D, typename E>
log_stream& operator << (log_stream& os, const std::tuple<A, B, C, D, E>& t) {
    os << '(' << std::get<0>(t) << ',' << std::get<1>(t)
       << ',' << std::get<2>(t) << ',' << std::get<3>(t)
       << ',' << std::get<4>(t) << ')';
    return os;
}

template <typename A, typename B, typename C, typename D, typename E, typename F>
log_stream& operator << (log_stream& os,
                         const std::tuple<A, B, C, D, E, F>& t) {
    os << '(' << std::get<0>(t) << ',' << std::get<1>(t)
       << ',' << std::get<2>(t) << ',' << std::get<3>(t)
       << ',' << std::get<4>(t) << ',' << std::get<5>(t) << ')';
    return os;
}

//! Logging helper to print arrays as [a1,a2,a3,...]
template <typename T, size_t N>
log_stream& operator << (log_stream& os, const std::array<T, N>& data) {
    os << '[';
    for (typename std::array<T, N>::const_iterator it = data.begin();
         it != data.end(); ++it)
    {
        if (it != data.begin()) os << ',';
        os << *it;
    }
    os << ']';
    return os;
}

//! Logging helper to print vectors as [a1,a2,a3,...]
template <typename T>
log_stream& operator << (log_stream& os, const std::vector<T>& data) {
    os << '[';
    for (typename std::vector<T>::const_iterator it = data.begin();
         it != data.end(); ++it)
    {
        if (it != data.begin()) os << ',';
        os << *it;
    }
    os << ']';
    return os;
}

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_LOGGER_HEADER

/******************************************************************************/
