/*******************************************************************************
 * thrill/common/logger.hpp
 *
 * Simple and less simple logging classes.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_LOGGER_HEADER
#define THRILL_COMMON_LOGGER_HEADER

#include <sstream>
#include <stdexcept>
#include <string>

namespace thrill {
namespace common {

//! Defines a name for the current thread, only if no name was set previously
void NameThisThread(const std::string& name);

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

There are two variation of \ref LOG and \ref sLOG: append 0 or 1 for temporarily
disabled or enabled debug lines. These macros are then \ref LOG0, \ref LOG1,
\ref sLOG0, and \ref sLOG1. The suffix overrides the debug variable's setting.

After a module works as intended, one can just set `debug = false`, and all
debug output will disappear.

## Critique of LOG and sLOG

The macros are only for rapid module-based development. It cannot be used as an
extended logging system for our network framework, where logs of network
execution and communication are collected for later analysis. Something else is
needed here.

 */
template <bool Active>
class Logger
{ };

template <>
class Logger<true>
{
protected:
    //! collector stream
    std::ostringstream oss_;

    //! for access to mutex_
    template <bool Active>
    friend class SpacingLogger;

public:
    //! Real active flag
    static const bool active = true;

    Logger() {
        oss_ << "[" << GetNameForThisThread() << "] ";
    }

    //! output any type, including io manipulators
    template <typename AnyType>
    Logger& operator << (const AnyType& at) {
        oss_ << at;
        return *this;
    }

    //! destructor: output a newline
    ~Logger();
};

template <>
class Logger<false>
{
public:
    //! Real active flag
    static const bool active = false;

    //! output any type, including io manipulators
    template <typename AnyType>
    Logger& operator << (const AnyType&) {
        return *this;
    }
};

/*!
 * A logging class which outputs spaces between elements pushed via
 * operator<<. Depending on the real parameter the output may be suppressed.
 */
template <bool Real>
class SpacingLogger
{ };

template <>
class SpacingLogger<true>
{
protected:
    //! true until the first element it outputted.
    bool first_ = true;

    //! collector stream
    std::ostringstream oss_;

public:
    //! Real active flag
    static const bool active = true;

    //! constructor: if real = false the output is suppressed.
    SpacingLogger() {
        oss_ << "[" << GetNameForThisThread() << "] ";
    }

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

template <>
class SpacingLogger<false>
{
public:
    //! Real active flag
    static const bool active = false;

    //! output any type, including io manipulators
    template <typename AnyType>
    SpacingLogger& operator << (const AnyType&) {
        return *this;
    }
};

// //! Default logging method: output if the local debug variable is true.
#define LOG ::thrill::common::Logger<debug>()

// //! Override default output: never or always output log.
#define LOG0 ::thrill::common::Logger<false>()
#define LOG1 ::thrill::common::Logger<true>()

// //! Explicitly specify the condition for logging
#define LOGC(cond) ::thrill::common::Logger<cond>()

//! Default logging method: output if the local debug variable is true.
#define sLOG ::thrill::common::SpacingLogger<debug>()

//! Override default output: never or always output log.
#define sLOG0 ::thrill::common::SpacingLogger<false>()
#define sLOG1 ::thrill::common::SpacingLogger<true>()

//! Explicitly specify the condition for logging
#define sLOGC(cond) ::thrill::common::SpacingLogger(cond)

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
#define die_unequal(X, Y)                         \
    do {                                          \
        if ((X) != (Y))                           \
            die("Inequality: " #X " != " #Y " : " \
                "\"" << "\" != \"" << Y << "\""); \
    } while (0)

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_LOGGER_HEADER

/******************************************************************************/
