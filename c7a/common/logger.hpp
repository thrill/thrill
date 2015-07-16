/*******************************************************************************
 * c7a/common/logger.hpp
 *
 * Simple and less simple logging classes.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_LOGGER_HEADER
#define C7A_COMMON_LOGGER_HEADER

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <mutex>
#include <thread>
#include <map>

namespace c7a {
namespace common {

//! thread-id to name mapping for better multi-threaded log output
class ThreadNameDirectory
{
public:
    //! Defines a name for the current thread, only if no name was set
    //! previously
    void NameThisThread(const std::string& name) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!HasNameForThisThread())
            thread_names_[std::this_thread::get_id()] = name;
    }

    //! True if name was defined for the current thread
    bool HasNameForThisThread() {
        return thread_names_.find(std::this_thread::get_id()) != thread_names_.end();
    }

    //! Returns the name of the current thread or 'unknown [id]'
    std::string NameForThisThread() {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = thread_names_.find(std::this_thread::get_id());
        if (it != thread_names_.end()) {
            return it->second;
        }
        std::ostringstream ss;
        ss << "unknown " << std::this_thread::get_id();
        return ss.str();
    }

private:
    std::map<std::thread::id, std::string> thread_names_;
    std::mutex mutex_;
};

//! access global singleton
ThreadNameDirectory & GetThreadDirectory();

/*!
 * A simple logging class which outputs a std::endl during destruction.
 * Depending on the real parameter the output may be suppressed.
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

    //! the global mutex of logger and spacing logger
    static std::mutex mutex_;

    //! for access to mutex_
    template <bool Active>
    friend class SpacingLogger;

public:
    //! Real active flag
    static const bool active = true;

    Logger() {
        oss_ << "[" << GetThreadDirectory().NameForThisThread() << "] ";
    }

    //! output any type, including io manipulators
    template <typename AnyType>
    Logger& operator << (const AnyType& at) {
        oss_ << at;
        return *this;
    }

    //! destructor: output a newline
    ~Logger() {
        oss_ << "\n";
        // lock the global mutex of logger for serialized output in
        // multi-threaded programs.
        std::unique_lock<std::mutex> lock;
        std::cout << oss_.str();
        std::cout.flush();
    }
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
    bool first_;

    //! collector stream
    std::ostringstream oss_;

public:
    //! Real active flag
    static const bool active = true;

    //! constructor: if real = false the output is suppressed.
    SpacingLogger()
        : first_(true) {
        oss_ << "[" << GetThreadDirectory().NameForThisThread() << "] ";
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
    ~SpacingLogger() {
        oss_ << "\n";
        // lock the global mutex of logger for serialized output in
        // multi-threaded programs.
        std::unique_lock<std::mutex> lock;
        std::cout << oss_.str();
        std::cout.flush();
    }
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
#define LOG ::c7a::common::Logger<debug>()

// //! Override default output: never or always output log.
#define LOG0 ::c7a::common::Logger<false>()
#define LOG1 ::c7a::common::Logger<true>()

// //! Explicitly specify the condition for logging
#define LOGC(cond) ::c7a::common::Logger<cond>()

//! Default logging method: output if the local debug variable is true.
#define sLOG ::c7a::common::SpacingLogger<debug>()

//! Override default output: never or always output log.
#define sLOG0 ::c7a::common::SpacingLogger<false>()
#define sLOG1 ::c7a::common::SpacingLogger<true>()

//! Explicitly specify the condition for logging
#define sLOGC(cond) ::c7a::common::SpacingLogger(cond)

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
} // namespace c7a

#endif // !C7A_COMMON_LOGGER_HEADER

/******************************************************************************/
