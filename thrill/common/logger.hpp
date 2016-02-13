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

#include <sstream>
#include <stdexcept>
#include <string>

namespace thrill {
namespace common {

//! memory manager singleton for Logger
extern mem::Manager g_logger_mem_manager;

template <typename Type>
using LoggerAllocator = mem::FixedAllocator<Type, g_logger_mem_manager>;

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
    static const bool debug = true;

    void func1()
    {
        LOG << "Hello World";

        LOG0 << "This is temporarily disabled.";
    }

    void func2()
    {
        static const bool debug = false;
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
    std::basic_ostringstream<
        char, std::char_traits<char>, LoggerAllocator<char> > oss_;

public:
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
    std::basic_ostringstream<
        char, std::char_traits<char>, LoggerAllocator<char> > oss_;

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

//! Instead of abort(), throw the output the message via an exception.
#define die(msg)                                            \
    do {                                                    \
        std::ostringstream oss;                             \
        oss << msg << " @ " << __FILE__ << ':' << __LINE__; \
        throw std::runtime_error(oss.str());                \
    } while (0)

//! Check condition X and die miserably if false. Same as assert() except this
//! is also active in Release mode.
#define die_unless(X) \
    do { if (!(X)) die("Assertion \"" #X "\" failed"); } while (0)

//! Check that X == Y or die miserably, but output the values of X and Y for
//! better debugging.
#define die_unequal(X, Y)                              \
    do {                                               \
        if ((X) != (Y))                                \
            die("Inequality: " #X " != " #Y " : "      \
                "\"" << X << "\" != \"" << Y << "\""); \
    } while (0)

//! Check that code throws an Exception
#define die_unless_throws(code, Exception)                        \
    do {                                                          \
        bool t_ = false; try { code; }                            \
        catch (const Exception&) { t_ = true; }                   \
        if (t_) break;                                            \
        die("UNLESS-THROWS: " #code " - NO EXCEPTION " #Exception \
            " @ " __FILE__ ":" << __LINE__);                      \
        abort();                                                  \
    } while (0)

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_LOGGER_HEADER

/******************************************************************************/
